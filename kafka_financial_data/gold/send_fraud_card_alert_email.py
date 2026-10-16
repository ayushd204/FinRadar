"""
FinRadar Fraud Card Alert — Email Notification (SDP Pipeline Integration)

Streaming table that reads NEW alerts from fraud_card_alert,
sends a summary email via SMTP for each micro-batch, and logs the sent
alert IDs. Only ONE table is used (alert_email_log) — streaming handles
deduplication via checkpointing, so no manual tracking table is needed.
"""

from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from datetime import datetime

SMTP_HOST    = "smtp.gmail.com"
SMTP_PORT    = 587
SENDER       = "ayushdhungana204@gmail.com"
RECEIVER     = "glandon991@gmail.com"
APP_PASSWORD = dbutils.secrets.get(scope="finradar_scope", key="gmail_api_key")


def send_and_log(iterator):
    """Pandas function: sends an alert for new fraud card entries, yields log entries."""
    import pandas as pd
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    for pdf in iterator:
        if pdf.empty:
            yield pd.DataFrame({"alert_id": pd.Series(dtype="str"),
                               "email_sent_at": pd.Series(dtype="datetime64[ns]")})
            continue

        count = len(pdf)
        rows_html = ""
        for _, r in pdf.iterrows():
            rows_html += f"""
            <tr>
              <td>{r['alert_id']}</td>
              <td>{r['customer_name']}</td>
              <td>{r['card_number']}</td>
              <td>{r['amount']:,.2f} {r['currency']}</td>
              <td>{r['merchant_name']}</td>
              <td>{r['merchant_category']}</td>
              <td>{r['watch_type']}</td>
              <td>{r['risk_level']}</td>
              <td>{r['action']}</td>
              <td>{r['reason_description']}</td>
              <td>{r['transaction_city']}, {r['transaction_country']}</td>
              <td>{r['transaction_timestamp']}</td>
            </tr>"""

        body = f"""
        <html><body>
        <h2>🚨 FinRadar Fraud Card Alert — {count} New Watchlist Match(es)</h2>
        <p>The following transactions matched a card on the fraud watchlist:</p>
        <table border="1" cellpadding="6" cellspacing="0"
               style="border-collapse:collapse;font-size:13px;">
          <tr style="background-color:#f2f2f2;">
            <th>Alert ID</th><th>Customer</th><th>Card Number</th>
            <th>Amount</th><th>Merchant</th><th>Category</th>
            <th>Watch Type</th><th>Risk Level</th><th>Action</th>
            <th>Reason</th><th>Location</th><th>Time</th>
          </tr>
          {rows_html}
        </table>
        <br><p>Automated alert from <b>FinRadar</b>.</p>
        </body></html>
        """

        msg = MIMEMultipart()
        msg["From"] = SENDER
        msg["To"] = RECEIVER
        msg["Subject"] = f"FinRadar Fraud Card Alert — {count} New Watchlist Match(es)"
        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER, APP_PASSWORD)
            server.send_message(msg)

        yield pd.DataFrame({
            "alert_id": pdf["alert_id"].tolist(),
            "email_sent_at": [datetime.now()] * count,
        })


@dp.append_flow(
    target="finradar.gold.alert_email_log",
    name="fraud_card_alert_email",
    comment="Send email notifications for fraud card watchlist alerts."
)
def fraud_card_alert_email() -> DataFrame:
    """Append flow: processes only NEW fraud card alerts (no duplicates via checkpointing)."""
    alerts = spark.readStream.table("finradar.gold.fraud_card_alert")
    return alerts.mapInPandas(
        send_and_log,
        schema="alert_id STRING, email_sent_at TIMESTAMP",
    )