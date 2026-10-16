# Databricks notebook source

body = """
<html>
<body>

<h2>Test Email from Databricks</h2>

<p>This is a test email sent from Databricks notebook.</p>

<p>If you receive this, the email configuration is working correctly.</p>

<br>

<p>
Thanks<br>
<b>FinRadar Support Team</b>
</p>

</body>
</html>
"""

print(body)


# COMMAND ----------

subject = "Test Email from Databricks"

print(subject)


gmail_api_key = dbutils.secrets.get(
            scope="finradar_scope",
            key="gmail_api_key"
        )

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

EMAIL = "ayushdhungana204@gmail.com"
APP_PASSWORD = gmail_api_key

# Receiver
to_email = "glandon991@gmail.com"

# Create message
msg = MIMEMultipart()
msg["From"] = EMAIL
msg["To"] = to_email
msg["Subject"] = subject

msg.attach(MIMEText(body, "html"))

try:
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL, APP_PASSWORD)
        server.send_message(msg)

    print("Email sent successfully!")

except Exception as e:
    print("Error sending email:", e)

# COMMAND ----------

