import time
import csv
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


URL = "https://kenmare.port-log.net/live/Display.php?Site=148"

def iniciar_driver():
    options = Options()
    options.add_argument("--headless")  # correr sem abrir browser
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver


def extrair_dados(driver):
    time.sleep(5)  # esperar carregar JS

    dados = {}

    # ======================
    # 🌊 TIDES
    # ======================
    try:
        dados["tide_observed"] = driver.find_element(By.XPATH, "//td[contains(text(),'Tides')]/following::td[1]").text
        dados["tide_predicted"] = driver.find_element(By.XPATH, "//td[contains(text(),'Tides')]/following::td[2]").text
        dados["tide_surge"] = driver.find_element(By.XPATH, "//td[contains(text(),'Tides')]/following::td[3]").text
    except:
        pass

    # ======================
    # 🌬️ WIND
    # ======================
    try:
        dados["wind_direction"] = driver.find_element(By.XPATH, "//td[contains(text(),'Wind')]/following::td[1]").text
        dados["wind_speed"] = driver.find_element(By.XPATH, "//td[contains(text(),'Wind')]/following::td[2]").text
        dados["gust_speed"] = driver.find_element(By.XPATH, "//td[contains(text(),'Wind')]/following::td[3]").text
        dados["gust_direction"] = driver.find_element(By.XPATH, "//td[contains(text(),'Wind')]/following::td[4]").text
    except:
        pass

    # ======================
    # 🌡️ MET
    # ======================
    try:
        dados["pressure"] = driver.find_element(By.XPATH, "//td[contains(text(),'Met')]/following::td[1]").text
        dados["temperature"] = driver.find_element(By.XPATH, "//td[contains(text(),'Met')]/following::td[2]").text
        dados["humidity"] = driver.find_element(By.XPATH, "//td[contains(text(),'Met')]/following::td[3]").text
        dados["dew_point"] = driver.find_element(By.XPATH, "//td[contains(text(),'Met')]/following::td[4]").text
        dados["precipitation"] = driver.find_element(By.XPATH, "//td[contains(text(),'Met')]/following::td[5]").text
    except:
        pass

    # ======================
    # 🌊 CURRENTS
    # ======================
    try:
        dados["current_direction"] = driver.find_element(By.XPATH, "//td[contains(text(),'Currents')]/following::td[1]").text
        dados["current_speed"] = driver.find_element(By.XPATH, "//td[contains(text(),'Currents')]/following::td[2]").text
    except:
        pass

    # Timestamp
    dados["timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    return dados


def salvar_csv(dados, ficheiro="dados_porto.csv"):
    headers = list(dados.keys())

    try:
        with open(ficheiro, "x", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerow(dados)
    except FileExistsError:
        with open(ficheiro, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerow(dados)


def main():
    driver = iniciar_driver()
    driver.get(URL)

    dados = extrair_dados(driver)
    print(dados)

    salvar_csv(dados)

    driver.quit()


if __name__ == "__main__":
    main()