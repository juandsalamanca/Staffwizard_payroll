import requests

url = "http://0.0.0.0:8000/health"
response = requests.get(url)
if response.status_code == 200:
    print(response.text)

    url = "http://0.0.0.0:8000/process_payroll"
    response = requests.post(url, files={"input_file": open("./static_data/ADP - GTS .xlsx", "rb")})

    if response.status_code == 200:
        with open("static_data/results.zip", "wb") as f:
            f.write(response.content)
        print(f"Output file saved to static_data/results.zip")
    else:
        print(response.status_code)