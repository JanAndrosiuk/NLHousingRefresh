import requests
from bs4 import BeautifulSoup
import re
import hashlib
import sqlite3
import smtplib
import ssl
from email.message import EmailMessage
import configparser
import logging
from logging.handlers import TimedRotatingFileHandler
import time
from datetime import datetime
CONFIG_PATH = "config.ini"


class HousingRefresh:

    def __init__(self, config_path=CONFIG_PATH) -> None:
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        print(self.config["all"])

        # Set Logger
        logging.basicConfig(
            # filename="../reports/logs/logger_"+time.strftime("%Y%m%d-%H%M%S")+".log",
            level=int(self.config["all"]["logger_level"]),
            format="%(asctime)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self.logger = logging.getLogger("Main")
        log_handler = TimedRotatingFileHandler(
            self.config["all"]["logger_path"] + datetime.today().strftime('%Y%m%d') + ".log", when="midnight",
            backupCount=30)
        log_handler.setLevel(int(self.config["all"]["logger_level"]))
        log_handler_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
        log_handler.suffix = "%Y%m%d"
        log_handler.setFormatter(log_handler_formatter)
        self.logger.addHandler(log_handler)
        self.logger.info("STARTED")

        self.url_yourhouse = self.config["all"]["url_yourhouse"]
        self.house_dict = None

    def get_current_posts(self) -> int:
        
        req = requests.get(self.url_yourhouse)
        soup = BeautifulSoup(req.text, "html.parser")
        newest_post = soup.find("article", {"class": "objectcontainer col-12 col-xs-12 col-sm-6 col-md-6 col-lg-4"})
        if newest_post is not None or newest_post != "":
            self.logger.info("Received YourHouse response")
        else:
            self.logger.info("Failed to receive response")

        self.house_dict = {
            "Address": soup.find("span", {"class": "street"}).text,
            "Price": re.findall(r'\d+', soup.find("span", {"class": "obj_price"}).text)[0],
            "Zipcode": soup.find("span", {"class": "zipcode"}).text,
            "URL": "https://your-house.nl/" + soup.find('a', {"class": "img-container"}, href=True)["href"],
            "SQM": re.findall(r'\d+', soup.find("span", {"class": "object_label object_sqfeet"}).text)[0],
            "Hash": "NULL",
            "LastUpdated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "_ResponseFullyCorrupt": False
        }

        # If there are None values, assign NULL to None values
        null_counter = 0
        for k, v in self.house_dict.items():
            if k == "Hash":
                break
            elif v is None:
                self.house_dict[k] = 'NULL'
                null_counter += 1
        if null_counter == 5:
            self.logger.info("No information retrieved. It is possible that the IP got blocked.")

        # Hash information from the row
        row_to_hash = self.house_dict["Price"] + self.house_dict["Address"] + self.house_dict["Zipcode"] + \
            self.house_dict["SQM"]
        self.house_dict["Hash"] = hashlib.md5(row_to_hash.encode()).hexdigest()

        return 0

    def prepare_database(self) -> int:
        con = sqlite3.connect(self.config["all"]["db_housing_path"],
                              detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS your_house (
            ID INTEGER PRIMARY KEY,
            Price VARCHAR(100) DEFAULT NULL,
            Address VARCHAR(1000) DEFAULT NULL,
            ZipCode VARCHAR(1000) DEFAULT NULL,
            Surface VARCHAR(100) DEFAULT NULL,
            URL VARCHAR(4000) DEFAULT NULL,
            Hash VARCHAR(4000) DEFAULT NULL,
            LastUpdated TIMESTAMP
        );
        """)
        con.commit()
        cur.close()
        con.close()

        self.logger.info("Preparing Database - Created the table (if didn't exist)")

        return 0

    def send_email(self) -> int:
        port = 465  # For SSL
        smtp_server = "smtp.gmail.com"
        sender_email = self.config["all"]["sender_email"]
        receiver_email = self.config["all"]["receiver_email"]
        password = self.config["all"]["dev_mail_password"]

        message = f"""\
        Price: {self.house_dict["Price"]}
        Address: {self.house_dict["Address"]}
        Zipcode: {self.house_dict["Zipcode"]}
        SQM: {self.house_dict["SQM"]}m2
        Last Updated: {self.house_dict["LastUpdated"]}
        URL: {self.house_dict["URL"]}
        """

        msg = EmailMessage()
        msg['Subject'] = "YourHouse refresh - new post"
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg.set_content(message)

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            # server.sendmail(sender_email, receiver_email, message)
            server.send_message(msg)

        return 0

    def check_for_changes(self) -> int:

        refresh_time = int(self.config["all"]["refresh_time"])

        # Create the table if it doesn't exist
        self.prepare_database()

        con = sqlite3.connect(self.config["all"]["db_housing_path"],
                              detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        cur = con.cursor()

        while True:

            # Populate house dictionary with current data
            self.get_current_posts()

            #  Get the latest hash from database
            previous_hash = cur.execute("""
                SELECT Hash
                FROM your_house
                ORDER BY ID DESC
                LIMIT 1
                ;
            """).fetchone()
            current_hash = self.house_dict["Hash"]

            # if no records (no previous hash), insert the current dictionary
            if previous_hash is None:
                cur.execute(f"""
                    INSERT INTO your_house (Price, Address, ZipCode, Surface, URL, Hash, LastUpdated)
                    VALUES(
                        '{self.house_dict["Price"]}',
                        '{self.house_dict["Address"]}',
                        '{self.house_dict["Zipcode"]}',
                        '{self.house_dict["SQM"]}',
                        '{self.house_dict["URL"]}',
                        '{self.house_dict["Hash"]}',
                        '{self.house_dict["LastUpdated"]}'
                    );
                """)
                con.commit()
                self.logger.info(f"Inserted new record (previous hash was None)")
                self.logger.info(f"waiting {refresh_time} seconds...")
                time.sleep(refresh_time)

            # if record already exists
            elif previous_hash[0] == current_hash:
                self.logger.info(f"No update, waiting {refresh_time} seconds...")
                time.sleep(refresh_time)

            # if new record
            elif previous_hash[0] != current_hash:

                # Send notification via dev e-mail
                self.send_email()
                self.logger.info("New record, E-Mail sent")
                self.logger.info(
                    f"{self.house_dict['Price']} - {self.house_dict['Address']} - {self.house_dict['Zipcode']} - \
                    {self.house_dict['SQM']}")

                # insert new record to database
                cur.execute(f"""
                    INSERT INTO your_house (Price, Address, ZipCode, Surface, URL, Hash, LastUpdated)
                    VALUES(
                        '{self.house_dict["Price"]}',
                        '{self.house_dict["Address"]}',
                        '{self.house_dict["Zipcode"]}',
                        '{self.house_dict["SQM"]}',
                        '{self.house_dict["URL"]}',
                        '{self.house_dict["Hash"]}',
                        '{self.house_dict["LastUpdated"]}'
                    );
                """)
                con.commit()
                self.logger.info("Inserted new record to the database")
                self.logger.info(f"waiting {refresh_time} seconds...")
                time.sleep(refresh_time)

        cur.close() # noqa
        con.close()

        return 0
