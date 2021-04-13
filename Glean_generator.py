import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange
spark = SparkSession.builder.master("local[*]").appName("glean_generator").getOrCreate()


class Glean_generator:
    """Glean generator initialized with two input csv and output csv path
       Instance methods implemented in the class to trigger glean in four situations
    """
    def __init__(self, invoice_csv_path, line_item_csv_path, output_csv_path):
        """Initialize glean_generator with invoice csv, line item csv and output csv path
        Args:
            invoice_csv_path: path of invoice csv file
            line_item_csv_path: path of line item csv file
            output_csv_path: target path of output csv file
        """
        self.df_invoice = spark.read.option("header", True).csv(invoice_csv_path).na.drop()
        self.df_line_item = spark.read.option("header", True).csv(line_item_csv_path).na.drop()
        self.output_schema = [
            "glean_id",
            "glean_date",
            "glean_text",
            "glean_type",
            "glean_location",
            "invoice_id",
            "canonical_vendor_id"
        ]
        self.glean_id = 0
        self.generated_gleans = []
        self.output_csv_path = output_csv_path

    def generate_glean(self, g_date, g_text, g_type, g_location, invoice_id, vendor_id):
        """Append all info needed for glean to generated gleans list
        """
        glean = (self.glean_id, g_date, g_text, g_type, g_location, invoice_id, vendor_id)
        self.generated_gleans.append(glean)
        self.glean_id += 1

    def trigger_vendor_not_seen_in_a_while(self):
        """Trigger vendor_not_seen_in_a_while situations
        """
        window = Window.partitionBy("canonical_vendor_id").orderBy("invoice_date")
        # get needed columns
        df = self.df_invoice.select(["invoice_id", "invoice_date", "canonical_vendor_id"])
        # create new columns for additional information needed
        df = df.withColumn("prev_invoice_date", F.lag(df.invoice_date).over(window))
        # calculate date differences in days between current invoice and the last invoice
        df = df.withColumn("days_between", F.datediff(df.invoice_date, df.prev_invoice_date))
        df = df.withColumn("cur_month", F.trunc("invoice_date", "month"))
        df = df.withColumn("prev_month", F.trunc("prev_invoice_date", "month"))
        # calculate date differences in months between current invoice and the last invoice
        df = df.withColumn("months_between", F.months_between("cur_month", "prev_month"))
        func = self.check_vendor_not_seen_in_a_while
        # map reduce checking function to generate result list
        result = df.rdd.map(lambda x: func(x)).reduce(lambda accum_list, cur_list: accum_list + cur_list)
        for g_date, g_text, g_type, g_location, invoice_id, vendor_id in result:
            self.generate_glean(g_date, g_text, g_type, g_location, invoice_id, vendor_id)

    @staticmethod
    def check_vendor_not_seen_in_a_while(row):
        """Function to check if vendor_not_seen_in_a_while situation triggered
           according to information in the row
        """
        days_between = row["days_between"]

        if days_between and days_between > 90:
            # trigger glean generation
            invoice_id = row["invoice_id"]
            invoice_date = row["invoice_date"]
            vendor_id = row["canonical_vendor_id"]
            months_between = int(row["months_between"])
            g_date = invoice_date
            g_text = f"First new bill in {months_between} months from vendor {vendor_id}"
            g_type = "vendor_not_seen_in_a_while"
            g_location = "invoice"

            return [(g_date, g_text, g_type, g_location, invoice_id, vendor_id)]
        else:
            return []

    def trigger_accrual_alert(self):
        """Trigger accrual_alert situations
        """
        # get needed information
        invoice_period_end_dates = self.df_invoice.select(["invoice_id", "period_end_date"])
        line_item_period_end_dates = self.df_line_item.select(["invoice_id", "period_end_date"])
        df = invoice_period_end_dates.union(line_item_period_end_dates)
        # group by invoice_id to get the latest period_end_date of invoice and
        # line items in the invoice
        df = df.groupBy("invoice_id").agg(F.max("period_end_date"))
        invoices_information = self.df_invoice.select(["invoice_id", "invoice_date", "canonical_vendor_id"])
        # join invoices information needed for glean generation
        df = df.join(invoices_information, ["invoice_id"])
        # calculate date differences in days between current invoice and the last invoice
        df = df.withColumn("days_between", F.datediff(df["max(period_end_date)"], df.invoice_date))
        func = self.check_accrual_alert
        # map reduce checking function to generate result list
        result = df.rdd.map(lambda x: func(x)).reduce(lambda accum_list, cur_list: accum_list + cur_list)
        for g_date, g_text, g_type, g_location, invoice_id, vendor_id in result:
            self.generate_glean(g_date, g_text, g_type, g_location, invoice_id, vendor_id)

    @staticmethod
    def check_accrual_alert(row):
        """Function to check if accrual_alert situation triggered
           according to information in the row
        """
        days_between = row["days_between"]

        if days_between and days_between > 90:
            invoice_id = row["invoice_id"]
            invoice_date = row["invoice_date"]
            vendor_id = row["canonical_vendor_id"]
            period_end_date = row["max(period_end_date)"]
            g_date = invoice_date
            g_text = f"Line items from vendor {vendor_id} in this invoice cover future periods (through {period_end_date})"
            g_type = "accrual_alert"
            g_location = "invoice"

            return [(g_date, g_text, g_type, g_location, invoice_id, vendor_id)]
        else:
            return []

    def trigger_large_month_increase_mtd(self):
        """Trigger large_month_increase_mtd situations
        """
        df = self.df_invoice.select(["invoice_id", "invoice_date", "total_amount", "canonical_vendor_id"])
        # create monthly data with mapping all data in the same month
        # to the first day of the month
        df = df.withColumn("first_date", F.trunc("invoice_date", "month"))
        # group by monthly and sum up the monthly spend
        df = df.groupBy(["canonical_vendor_id", "first_date"]).agg(F.sum("total_amount"))
        # calculate the yearly average spend in the last year
        window = Window.partitionBy("canonical_vendor_id") \
                       .orderBy(F.col("first_date").cast("timestamp").cast("int")) \
                       .rangeBetween(-self.days2seconds(365), 0)
        df = df.withColumn("avg_amount_last_year", F.mean(df["sum(total_amount)"]).over(window))
        df = df.withColumn("year", F.year(df.first_date))
        df = df.withColumn("month", F.month(df.first_date))
        func = self.check_large_month_increase_mtd
        # map reduce checking function to generate result list
        result = df.rdd.map(lambda x: func(x)).reduce(lambda accum_list, cur_list: accum_list + cur_list)
        for g_date, g_text, g_type, g_location, invoice_id, vendor_id in result:
            self.generate_glean(g_date, g_text, g_type, g_location, invoice_id, vendor_id)

    @staticmethod
    def check_large_month_increase_mtd(row):
        """Function to check if large_month_increase_mtd situation triggered
           according to information in the row
        """
        try:
            monthly_spend = row["sum(total_amount)"]
        except Exception as e:
            print(f"Raised exception in check_large_month_increase_mtd: {e}")
        avg_spend_last_year = row["avg_amount_last_year"]
        spend_diff = monthly_spend - avg_spend_last_year
        increased_rate = spend_diff / avg_spend_last_year

        triggered = False
        if monthly_spend > 10000 and increased_rate > 0.5:
            triggered = True
        elif monthly_spend > 1000 and increased_rate > 2:
            triggered = True
        elif monthly_spend > 100 and increased_rate > 5:
            triggered = True

        if not triggered:
            # Glean not triggered, return empty list
            return []

        invoice_id = ""
        vendor_id = row["canonical_vendor_id"]
        year = row["year"]
        month = row["month"]
        g_date = f"{year}-{month}"
        g_text = f"Monthly spend with {vendor_id} is ${spend_diff:.2f} ({increased_rate*100:.2f}%) higher than average"
        g_type = "large_month_increase_mtd"
        g_location = "vendor"

        return [(g_date, g_text, g_type, g_location, invoice_id, vendor_id)]

    def trigger_no_invoice_received(self):
        """Trigger no_invoice_received situations
        """
        df = self.df_invoice.select(["invoice_id", "invoice_date", "canonical_vendor_id"])
        # get the day number of date when invoice received
        df_day_frequency = df.withColumn("frequent_day", F.dayofmonth("invoice_date"))
        # group by vendor id to calculate the frequency of days that the vendor
        # sends invoices
        df_day_frequency = df_day_frequency.groupBy(["canonical_vendor_id", "frequent_day"]) \
                                           .agg(F.count("frequent_day"))
        # sort by frequency and get the most frequent day
        window = Window.partitionBy("canonical_vendor_id").orderBy(["count_for_sort", "frequent_day"])
        # frequency need to sort by negative value to maintain ordering for the
        # requirement: most frequent day -> the earliest day
        df_day_frequency = df_day_frequency.withColumn("count_for_sort", -df_day_frequency["count(frequent_day)"])
        df_day_frequency = df_day_frequency.withColumn("rank", F.rank().over(window))
        df_day_frequency = df_day_frequency.filter(F.col("rank") == 1) \
                                           .drop("count(frequent_day)", "count_for_sort", "rank")
        # create monthly data with mapping all data in the same month
        # to the first day of the month
        df_with_first_date = df.withColumn("first_date", F.trunc("invoice_date", "month"))
        # label every month invoices received as 1
        df_received = df_with_first_date.withColumn("received", F.lit(1))
        df_received = df_received.select(["canonical_vendor_id", "first_date", "received"]).distinct()
        # accumulate labels within last three months for the convienience to check
        # if there are three consecutive months with invoices received
        window = Window.partitionBy("canonical_vendor_id") \
                       .orderBy(F.col("first_date").cast("timestamp").cast("int")) \
                       .rangeBetween(-self.days2seconds(92), -self.days2seconds(1))
        df_received = df_received.withColumn("prev_received", F.sum(df_received.received).over(window))
        df_received = df_received.join(df_day_frequency, ["canonical_vendor_id"])
        df_with_first_date = df_with_first_date.groupBy(["canonical_vendor_id", "first_date"]).agg(F.min("invoice_date"))
        df_received = df_received.join(df_with_first_date, ["canonical_vendor_id", "first_date"]).drop("received")
        func = self.return_no_invoice_received_results
        # map reduce to generate result list
        result = df_received.rdd.map(lambda x: func(x)).reduce(lambda accum_list, cur_list: accum_list + cur_list)
        # do all the statistics and logic checks to generate gleans
        self.check_no_invoice_received(result)

    @staticmethod
    def return_no_invoice_received_results(row):
        vendor_id = row["canonical_vendor_id"]
        prev_received_count = row["prev_received"]
        invoice_sent_day = row["frequent_day"]
        first_invoice_date = row["min(invoice_date)"]

        return [{"vendor_id": vendor_id,
                 "prev_received_count": prev_received_count,
                 "invoice_sent_day": invoice_sent_day,
                 "first_invoice_date": first_invoice_date}]

    def check_no_invoice_received(self, result_list):
        """Function to check if monthly or quarterly no_invoice_received situation
           triggered according to information in the result list
        """
        vendor_invoices_info = {}
        # organize result of vendor information to hash map
        # dict[vendor] -> dict["received_months"] -> dict["invoice_sent_day"] -> accumulated_previous_received
        #              -> dict["received_quarters"] -> set[received_quarter]
        for dic in result_list:
            date = datetime.strptime(dic["first_invoice_date"], "%Y-%m-%d")
            received_month = f"{date.year}-{date.month}"
            vendor_id = dic["vendor_id"]
            if vendor_id not in vendor_invoices_info:
                vendor_invoices_info[vendor_id] = {}
                vendor_invoices_info[vendor_id]["received_months"] = {}
                vendor_invoices_info[vendor_id]["received_quarters"] = set()
            vendor_invoices_info[vendor_id]["invoice_sent_day"] = dic["invoice_sent_day"]
            vendor_invoices_info[vendor_id]["received_months"][received_month] = dic["prev_received_count"]
            quarter = self.get_quarter(date)
            vendor_invoices_info[vendor_id]["received_quarters"].add(quarter)

        for vendor in vendor_invoices_info:
            info_dict = vendor_invoices_info[vendor]
            frequent_day = info_dict["invoice_sent_day"]

            # monthly basis calculations
            first_month = min(info_dict["received_months"].keys())
            first_month = datetime.strptime(first_month, "%Y-%m")
            last_month = max(info_dict["received_months"].keys())
            last_month = datetime.strptime(last_month, "%Y-%m") + relativedelta(months=1)

            cur_month = first_month + relativedelta(months=1)
            # check for every month until the last month
            while cur_month <= last_month:
                cur_month_key = f"{cur_month.year}-{cur_month.month}"
                if cur_month_key not in info_dict["received_months"]:
                    # invoice not received in current month
                    prev_month = cur_month + relativedelta(months=-1)
                    prev_month_key = f"{prev_month.year}-{prev_month.month}"
                    if prev_month_key in info_dict["received_months"]:
                        # invoice received in the last month
                        prev_received_count = info_dict["received_months"][prev_month_key]
                        # check if invoices received in three consecutive months
                        if prev_received_count == 3:
                            # trigger glean generation
                            month_range = monthrange(cur_month.year, cur_month.month)
                            end_of_month = max(month_range)
                            for day in range(frequent_day, end_of_month+1):
                                g_date = f"{cur_month.year}-{cur_month.month}-{day}"
                                g_text = f"{vendor_id} generally charges between on {frequent_day} day of each month invoices are sent. On {g_date}, an invoice from {vendor_id} has not been received"
                                g_type = "no_invoice_received"
                                g_location = "vendor"
                                invoice_id = ""
                                self.generate_glean(g_date, g_text, g_type, g_location, invoice_id, vendor_id)
                cur_month += relativedelta(months=1)

            # quarterly basis calculations
            received_quarter_set = info_dict["received_quarters"]
            first_quarter = min(received_quarter_set)
            last_quarter = max(received_quarter_set)
            last_quarter = self.quarter_addition(last_quarter, 1)

            cur_quarter = self.quarter_addition(first_quarter, 1)
            # check for every quarter until the last quarter
            while cur_quarter <= last_quarter:
                if cur_quarter not in received_quarter_set:
                    # invoice not received in current quarter
                    prev_quarter = self.quarter_substraction(cur_quarter, 1)
                    prev_prev_quarter = self.quarter_substraction(cur_quarter, 2)
                    # check if invoices received in two consecutive quarters
                    if prev_quarter in received_quarter_set and prev_prev_quarter in received_quarter_set:
                        # trigger glean generation
                        cur_quarter_month = int(self.first_month_of_quarter(cur_quarter))
                        cur_quarter_year = int(cur_quarter.split("-")[0])
                        month_range = monthrange(cur_quarter_year, cur_quarter_month)
                        end_of_month = max(month_range)
                        for day in range(frequent_day, end_of_month+1):
                            g_date = f"{cur_quarter_year}-{cur_quarter_month}-{day}"
                            g_text = f"{vendor_id} generally charges between on {frequent_day} day of each quarter invoices are sent. On {g_date}, an invoice from {vendor_id} has not been received"
                            g_type = "no_invoice_received"
                            g_location = "vendor"
                            invoice_id = ""
                            self.generate_glean(g_date, g_text, g_type, g_location, invoice_id, vendor_id)
                cur_quarter = self.quarter_addition(cur_quarter, 1)

    @staticmethod
    def days2seconds(day_count) -> int:
        """transform days count to seconds count
        """
        return day_count * 86400

    @staticmethod
    def get_quarter(date):
        """Get quarter number according to given date
        """
        month = int(date.month)
        year = date.year
        if 1 <= month <= 3:
            quarter = 1
        elif 4 <= month <= 6:
            quarter = 2
        elif 7 <= month <= 9:
            quarter = 3
        else:
            quarter = 4
        return f"{year}-{quarter}"

    @staticmethod
    def quarter_substraction(quarter_date, delta) -> str:
        """Function to substract quarter to input quarter date
        """
        year, quarter = quarter_date.split("-")
        year = int(year)
        quarter = int(quarter)
        while delta >= 4:
            year -= 1
            delta -= 4
        quarter -= delta
        return f"{year}-{quarter}"

    @staticmethod
    def quarter_addition(quarter_date, delta) -> str:
        """Function to add quarter to input quarter date
        """
        year, quarter = quarter_date.split("-")
        year = int(year)
        quarter = int(quarter)
        quarter += delta
        while quarter > 4:
            quarter -= 4
            year += 1
        return f"{year}-{quarter}"

    @staticmethod
    def first_month_of_quarter(quarter_date) -> str:
        """Get the first month of the input quarter
        """
        quarter = quarter_date.split("-")[1]
        quarter = int(quarter)
        return f"{1 + (quarter-1) * 3}"

    def trigger_all(self) -> None:
        """Trigger glean generation in all situations
        """
        self.trigger_vendor_not_seen_in_a_while()
        self.trigger_accrual_alert()
        self.trigger_large_month_increase_mtd()
        self.trigger_no_invoice_received()

    def write_csv(self) -> None:
        """Create dataframe with generated gleans and write to csv file
        """
        output_df = spark.createDataFrame(self.generated_gleans).toDF(*self.output_schema)
        output_df.toPandas().to_csv(self.output_csv_path, index=False, header=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--invoice_csv", help="path of invoice csv file")
    parser.add_argument("--line_item_csv", help="path of line item csv file")
    parser.add_argument("--output_csv", help="path of output csv file")
    args = parser.parse_args()

    invoice_csv_path = args.invoice_csv
    line_item_csv_path = args.line_item_csv
    output_csv_path = args.output_csv

    glean_generator = Glean_generator(
        invoice_csv_path=invoice_csv_path,
        line_item_csv_path=line_item_csv_path,
        output_csv_path=output_csv_path
    )

    glean_generator.trigger_all()
    glean_generator.write_csv()


if __name__ == "__main__":
    main()
