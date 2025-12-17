from email.message import MIMEPart
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import ssl
import core.config as cfg
from core.logger import get_logger

__all__ = ['init_email', 'send_email']


def send_email(to_emails, subject, content):
    try:
        context = ssl.create_default_context()
        server = smtplib.SMTP(host, int(port))
        message = MIMEText(content, 'html')

        email_list = []
        for email in to_emails.replace(';', ',').split(','):
            email_list.append(email)

        message["Subject"] = subject
        message["From"] = from_email
        message["To"] = to_emails
        server.starttls(context=context)
        server.login(user, password)
        server.set_debuglevel(1)
        server.sendmail(from_email, email_list, message.as_string())
    except Exception as err:
        logger.error(err)
    finally:
        server.quit()


def init_email():

    global host
    global port
    global user
    global password
    global from_email
    global to_emails
    global to_emails_report
    global logger

    logger = get_logger()
    host = cfg.get_parameter('Smtp_Server', "host_email")
    port = cfg.get_parameter('Smtp_Server', "port")
    user = cfg.get_parameter('Smtp_Server', "user")
    password = cfg.get_parameter('Smtp_Server', "password")
    from_email = cfg.get_parameter('Smtp_Server', "from_email")
    to_emails = cfg.get_parameter('Smtp_Server', "to_emails")
    to_emails_report = cfg.get_parameter('Smtp_Server', "to_emails_report")
