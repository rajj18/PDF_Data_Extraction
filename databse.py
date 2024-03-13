import sqlite3
import pdfplumber
from datetime import datetime, timedelta
import re


class Database:
    def __init__(self, database_file="transactions.db"):
        self.database_file = database_file
        self.create_table()

    def create_table(self):
        conn = self.get_connection()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS transactions
                     (id INTEGER PRIMARY KEY,
                     app_id TEXT,
                     xref TEXT UNIQUE,
                     settlement_date TEXT,
                     broker TEXT,
                     sub_broker TEXT,
                     borrower_name TEXT,
                     description TEXT,
                     total_loan_amount REAL,
                     commission_rate REAL,
                     upfront REAL,
                     upfront_incl_gst REAL,
                     tier_level TEXT)''')
        conn.commit()
        conn.close()

    def get_connection(self):
        return sqlite3.connect(self.database_file)

    def insert_transactions_from_pdf(self, pdf_file):
        transactions = self.extract_transactions(pdf_file)
        conn = self.get_connection()
        c = conn.cursor()
        try:
            for transaction in transactions:
                c.execute('''INSERT OR IGNORE INTO transactions
                             (app_id, xref, settlement_date, broker, sub_broker,
                             borrower_name, description, total_loan_amount,
                             commission_rate, upfront, upfront_incl_gst)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (transaction['app_id'], transaction['xref'],
                           transaction['settlement_date'], transaction['broker'],
                           transaction['sub_broker'], transaction['borrower_name'],
                           transaction['description'], transaction['total_loan_amount'],
                           transaction['commission_rate'], transaction['upfront'],
                           transaction['upfront_incl_gst']))
            conn.commit()
        finally:
            conn.close()

    def extract_transactions(self, pdf_file):
        transactions = []
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()

                # Define regex patterns to match transaction details
                app_id_pattern = r"App\s*ID:\s*(\w+)"
                xref_pattern = r"Xref:\s*(\w+)"
                settlement_date_pattern = r"Settlement\s*Date:\s*(\d{4}-\d{2}-\d{2})"
                broker_pattern = r"Broker:\s*(\w+)"
                sub_broker_pattern = r"Sub\s*Broker:\s*(\w+)"
                borrower_name_pattern = r"Borrower\s*Name:\s*(.+)"
                description_pattern = r"Description:\s*(.+)"
                total_loan_amount_pattern = r"Total\s*Loan\s*Amount:\s*(\d+\.\d+)"
                commission_rate_pattern = r"Commission\s*Rate:\s*(\d+\.\d+)"
                upfront_pattern = r"Upfront:\s*(\d+\.\d+)"
                upfront_incl_gst_pattern = r"Upfront\s*Incl\s*GST:\s*(\d+\.\d+)"

                # Search for patterns in the text
                app_id_match = re.search(app_id_pattern, text)
                xref_match = re.search(xref_pattern, text)
                settlement_date_match = re.search(settlement_date_pattern, text)
                broker_match = re.search(broker_pattern, text)
                sub_broker_match = re.search(sub_broker_pattern, text)
                borrower_name_match = re.search(borrower_name_pattern, text)
                description_match = re.search(description_pattern, text)
                total_loan_amount_match = re.search(total_loan_amount_pattern, text)
                commission_rate_match = re.search(commission_rate_pattern, text)
                upfront_match = re.search(upfront_pattern, text)
                upfront_incl_gst_match = re.search(upfront_incl_gst_pattern, text)

                # Extract transaction details from matches
                if (app_id_match and xref_match and settlement_date_match and broker_match
                        and sub_broker_match and borrower_name_match and description_match
                        and total_loan_amount_match and commission_rate_match
                        and upfront_match and upfront_incl_gst_match):
                    transaction = {
                        "app_id": app_id_match.group(1),
                        "xref": xref_match.group(1),
                        "settlement_date": settlement_date_match.group(1),
                        "broker": broker_match.group(1),
                        "sub_broker": sub_broker_match.group(1),
                        "borrower_name": borrower_name_match.group(1),
                        "description": description_match.group(1),
                        "total_loan_amount": float(total_loan_amount_match.group(1)),
                        "commission_rate": float(commission_rate_match.group(1)),
                        "upfront": float(upfront_match.group(1)),
                        "upfront_incl_gst": float(upfront_incl_gst_match.group(1))
                    }

                    transactions.append(transaction)
        return transactions

    def deduplicate_transactions(self):
        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.execute('''DELETE FROM transactions
                         WHERE id NOT IN (SELECT MIN(id) 
                                          FROM transactions 
                                          GROUP BY xref, total_loan_amount)''')
            conn.commit()
        finally:
            conn.close()

    def calculate_total_loan_amount(self, start_date: str, end_date: str):
        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.execute('''SELECT SUM(total_loan_amount) FROM transactions
                         WHERE settlement_date BETWEEN ? AND ?''',
                      (start_date, end_date))
            result = c.fetchone()[0]
            return result
        finally:
            conn.close()

    def calculate_highest_loan_amount_by_broker(self, broker: str):
        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.execute('''SELECT MAX(total_loan_amount) FROM transactions
                         WHERE broker = ?''', (broker,))
            result = c.fetchone()[0]
            return result
        finally:
            conn.close()

    def generate_broker_report(self, period: str):
        conn = self.get_connection()
        c = conn.cursor()

        if period == "daily":
            query = '''SELECT settlement_date, broker, SUM(total_loan_amount) AS total_loan_amount
                       FROM transactions
                       GROUP BY settlement_date, broker
                       ORDER BY settlement_date DESC, total_loan_amount DESC'''
        elif period == "weekly":
            # Generate report for the last week (7 days)
            start_date = datetime.now() - timedelta(days=7)
            start_date = start_date.strftime('%Y-%m-%d')
            query = '''SELECT strftime('%Y-%m-%d', settlement_date, 'weekday 0', '-7 days') AS week_start,
                              broker, SUM(total_loan_amount) AS total_loan_amount
                       FROM transactions
                       WHERE settlement_date >= ?
                       GROUP BY week_start, broker
                       ORDER BY week_start DESC, total_loan_amount DESC'''
            c.execute(query, (start_date,))
        elif period == "monthly":
            # Generate report for the last month
            start_date = datetime.now().replace(day=1) - timedelta(days=1)
            start_date = start_date.strftime('%Y-%m-%d')
            query = '''SELECT strftime('%Y-%m', settlement_date) AS month, broker, SUM(total_loan_amount) AS total_loan_amount
                       FROM transactions
                       WHERE settlement_date >= ?
                       GROUP BY month, broker
                       ORDER BY month DESC, total_loan_amount DESC'''
            c.execute(query, (start_date,))
        else:
            conn.close()
            raise ValueError("Invalid period. Please choose 'daily', 'weekly', or 'monthly'.")

        result = c.fetchall()
        conn.close()
        return result

    def generate_loan_amount_report(self):
        conn = self.get_connection()
        c = conn.cursor()
        c.execute('''SELECT settlement_date, SUM(total_loan_amount) AS total_loan_amount
                     FROM transactions
                     GROUP BY settlement_date
                     ORDER BY settlement_date ASC''')
        result = c.fetchall()
        conn.close()
        return result

    def define_tier_level(self):
        conn = self.get_connection()
        c = conn.cursor()
        c.execute('''UPDATE transactions
                     SET tier_level = 
                         CASE 
                             WHEN total_loan_amount > 100000 THEN 'Tier 1'
                             WHEN total_loan_amount > 50000 THEN 'Tier 2'
                             WHEN total_loan_amount > 10000 THEN 'Tier 3'
                             ELSE 'Tier 4'
                         END''')
        conn.commit()
        conn.close()

    def generate_tier_report(self):
        conn = self.get_connection()
        c = conn.cursor()
        c.execute('''SELECT settlement_date, tier_level, COUNT(*) AS count
                     FROM transactions
                     GROUP BY settlement_date, tier_level
                     ORDER BY settlement_date ASC, tier_level ASC''')
        result = c.fetchall()
        conn.close()
        return result
