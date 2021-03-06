from pyspark.sql.functions import col
from src.tools.DbCon import DbCon
from src.tools.Colors import Colors

spark = DbCon()


# Sub Menu For Recovery Data
def recovery_menu():
    print("""
    MAIN> RECOVERY DATA>
    1.) US Total Recovered Patients
    2.) Recovered Patients By State
    3.) US Vaccination Rates
    4.) Return To Main
    """)

    recovery_selector = input(f"{Colors.g.value}Please Select A Menu Option:{Colors.w.value}")

    if recovery_selector == "1":
        total_recovery()
    elif recovery_selector == "2":
        recovery_by_state()
    elif recovery_selector == "3":
        vaccination_rate()
    elif recovery_selector == "4":
        print("Returning To Main")
    else:
        print(f"{Colors.r}Invalid Selection. Please Try Again{Colors.w.value}")


# Create And Return Recovery DataFrame
def create_table():
    print("Creating Recovery Table")

    try:
        df_i = spark.con.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("hdfs://localhost:9000/user/narma/covid/data/time_series_covid19_confirmed_US.csv") \
            .select(col("UID"), col("Admin2"), col("Province_State"), col("1/21/22")) \
            .withColumnRenamed("UID", "id_i") \
            .withColumnRenamed("Admin2", "City_i") \
            .withColumnRenamed("Province_State", "State_i") \
            .withColumnRenamed("1/21/22", "1_21_22_i")

        df_d = spark.con.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("hdfs://localhost:9000/user/narma/covid/data/time_series_covid19_deaths_US.csv") \
            .select(col("UID"), col("Admin2"), col("Province_State"), col("1/21/22")) \
            .withColumnRenamed("UID", "id_d") \
            .withColumnRenamed("Admin2", "City_d") \
            .withColumnRenamed("Province_State", "State_d") \
            .withColumnRenamed("1/21/22", "1_21_22_d")

        df = df_i.join(df_d, col("id_i") == col("id_d"), "inner").createOrReplaceTempView("RecoveryInfo")
        return df
    except:
        print("File Not Found")
        return None


# Print Total Recovered Covid-19 Patients
def total_recovery():
    print("Total US Recovery\n")
    create_table()

    spark.con.sql("""
        SELECT (SUM(1_21_22_i) - SUM(1_21_22_d)) AS NumRecovered,
        ROUND((1 - SUM(1_21_22_d) / SUM(1_21_22_i)) * 100, 2) AS PercentRecovered
        FROM RecoveryInfo""").show()

    input("Enter Any Key To Return")
    spark.con.catalog.dropTempView("RecoveryInfo")


# Print Number of Recovered Patients By State
def recovery_by_state():
    print("Recovery By State\n")
    create_table()

    spark.con.sql("""
        SELECT State_d, (SUM(1_21_22_i) - SUM(1_21_22_d)) AS NumRecovered,
        ROUND((1 - SUM(1_21_22_d) / SUM(1_21_22_i)) * 100, 2) AS PercentRecovered
        FROM RecoveryInfo 
        WHERE State_d NOT LIKE('%Princess%')
        GROUP BY State_d ORDER BY PercentRecovered DESC""").show(100, False)

    input("Enter Any Key To Return")
    spark.con.catalog.dropTempView("RecoveryInfo")


# Print Total Vaccinations and Weekly Rates
def vaccination_rate():
    print("Vaccination Rates\n")
    spark.con.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("hdfs://localhost:9000/user/narma/covid/data/country_vaccinations.csv") \
        .select(col("country"), col("date"), col("people_fully_vaccinated")) \
        .where(col("country").startswith("United States")) \
        .createOrReplaceTempView("Vaccinations_Group")

    spark.con.sql("""
    SELECT CAST(MAX(people_fully_vaccinated) AS BIGINT) AS TotalVaccinated,
    ROUND((MAX(people_fully_vaccinated) / COUNT(people_fully_vaccinated)), 2) * 7 AS VaccinesPerWeek
    FROM Vaccinations_Group
    """).show(5, False)

    spark.con.catalog.dropTempView("TotalVaccinated")
    vaccinations_by_vendor()


def vaccinations_by_vendor():
    print("Vaccinations By Vendor\n")

    spark.con.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("hdfs://localhost:9000/user/narma/covid/data/country_vaccinations_by_manufacturer.csv") \
        .select(col("location"), col("vaccine"), col("total_vaccinations")) \
        .where(col("location").startswith("United States")) \
        .createOrReplaceTempView("VendorView")

    spark.con.sql("""
    SELECT vaccine, MAX(total_vaccinations) AS total_shots
    FROM VendorView
    GROUP BY vaccine
    """).show(20, False)

    input("Enter Any Key To Return")
    spark.con.catalog.dropTempView("VendorView")
