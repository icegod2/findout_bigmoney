import time
import datetime as dt
import json
import os
import errno
import itertools
import shutil
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

context = ssl.create_default_context()

class NotifyUser:
    # 建構式rank_up_day_cnt
    def __init__(self, smtp_server, port, passwd, sender):
        self.email_enable = True
        self.smtp_server = smtp_server
        self.smtp_port = port
        self.smtp_passwd = passwd
        self.sender_email = sender
        self.nofify_mail_list = []

    def add_notify_user_email(self, email):
        self.nofify_mail_list.append(email)

    def get_notify_user_email(self):
        return self.nofify_mail_list

    def print_smtp_info(self):
        print("smtp server:{}:{}".format(self.smtp_server, self.smtp_port))
        print("smtp sender_email:{}".format(self.sender_email))
        print("smtp passwd:{}".format(self.smtp_passwd))

    def list_notify_user_email(self, email):
        return self.nofify_mail_list

    def send_email(self, body):
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            try:
                server.ehlo()  # Can be omitted
                server.starttls(context=context)
                server.ehlo()  # Can be omitted
                server.login(self.sender_email, self.smtp_passwd)
                receiver_emails = ','.join(self.nofify_mail_list)
                print(receiver_emails)
                message = MIMEMultipart("alternative")
                message["Subject"] = "Today Stock list to buy"
                message["From"] = self.sender_email
                message["To"] = receiver_emails
                part = MIMEText(body, "html")
                message.attach(part)  
                server.sendmail(self.sender_email, receiver_emails, message.as_string())
            except Exception as e:
                print(e)
            finally:
                server.quit() 