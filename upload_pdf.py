import requests

url = "http://localhost:8000/upload/"
file_path = "C:/Users/rjajm/PycharmProjects/PDFdataextraction/Test_pdf.pdf"

files = {"file": open(file_path, "rb")}
response = requests.post(url, files=files)

print(response.json())
