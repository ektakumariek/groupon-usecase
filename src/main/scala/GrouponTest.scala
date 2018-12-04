import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.sql.Date

object GrouponTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Groupon Test")
      .getOrCreate()

    import sparkSession.implicits._

    // Registering the UDFs
    val dateFormatterUdf = udf[Date, String](dateFormatter)
    val grossBasketValueUdf = udf[Double, Int](grossBasketValue)

    // Customer Df
    var custDf = List(
      (100, "abc", "M", "14/01/1985"),
      (101, "def", "F", "1987-01-14"),
      (102, "ghi", "M", "1986-02-22")
    ).toDF("Customer_id", "name", "gender", "Dateofbirth")
    custDf = custDf.withColumn("Dateofbirth", dateFormatterUdf($"Dateofbirth"))
    custDf.createOrReplaceTempView("cust_vw")

    custDf.printSchema()
    custDf.show()

    // Transaction Df
    var txnDf = List(
      (5001, "2018-01-16", 100, 1000),
      (5002, "2018-01-16", 101, 1250),
      (5003, "2018-01-16", 102, 750),
      (5004, "2018-01-19", 100, 250),
      (5005, "2018-02-04", 102, 650),
      (5006, "2018-02-06", 102, 950),
      (5007, "2018-03-01", 103, 640)
    ).toDF("transaction_id", "Transaction_date", "Customer_id", "Basket_amount_in_cents")

    // Answer to Q3 - gross_basket_value
    txnDf = txnDf
      .withColumn("Transaction_date", dateFormatterUdf($"Transaction_date"))
        .withColumn("gross_basket_value", grossBasketValueUdf($"Basket_amount_in_cents"))

    txnDf.createOrReplaceTempView("txn_vw")

    txnDf.printSchema()
    txnDf.show()

    // Voucher Df
    var vouchersDf = List(
      (1100111212L, "2018-01-04", 200, 100),
      (2390324796L, "2018-02-01", 180, 101)
    ).toDF("Voucher_id", "Subscribed_date", "Voucher_value_in_cents", "Customer_id")
    vouchersDf = vouchersDf
        .withColumn("Subscribed_date", dateFormatterUdf($"Subscribed_date"))
    vouchersDf.printSchema()
    vouchersDf.show()

    // Answer to Q4 - Monthly hieghest transaction amount along with date of birth
    sparkSession.sql(
      """
        select
            a.Customer_id,
            a.txn_month,
            a.max_gross_basket_value,
            b.Dateofbirth
        from
            (select
                tbl.Customer_id,
                tbl.txn_month,
                max(tbl.gross_basket_value) max_gross_basket_value
             from
              (
                select
                  Customer_id,
                  concat(year(Transaction_date), "-", month(Transaction_date)) txn_month,
                  gross_basket_value
                from
                  txn_vw
              ) tbl
             group by
                Customer_id,
                txn_month
              ) a
              left join
              cust_vw b
              on a.Customer_id = b.Customer_id
      """.stripMargin)
      .show()

    // Answer to Q5 -
    sparkSession.sql(
      """
         select
          x.Customer_id,
          y.txn_month,
          x.gross_basket_value
         from
         txn_vw x
         join
        (select
          Customer_id,
          concat(year(Transaction_date), "-", month(Transaction_date)) txn_month
         from
          txn_vw
         group by
          Customer_id,
          concat(year(Transaction_date), "-", month(Transaction_date))
         having concat(year(Transaction_date), "-", month(Transaction_date)) = max(concat(year(Transaction_date), "-", month(Transaction_date)))
         ) y
         on x.Customer_id = y.Customer_id
      """.stripMargin
    ).show()

    sparkSession.stop()
  }


  // UDfs
  def dateFormatter(date: String): Date = {
    if(date.contains("/"))
     return new Date(new SimpleDateFormat("dd/MM/yyyy").parse(date).getTime)
    else
      return new Date(new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime)
  }

  def grossBasketValue(cents: Int): Double = {
    return (cents.toDouble / 100.0)
  }
}
