import pandas as pd
import requests
import io

# Create a sample DataFrame
data = {
    'sale_id': [9001, 9002],
    'sale_datetime': ['2023-12-01', '2023-12-02'],
    'region_name': ['North', 'South'],
    'city': ['Winterfell', 'Sunspear'],
    'manager': ['John Snow', 'Oberyn'],
    'product_id': [101, 102],
    'product_name': ['Sword', 'Spear'],
    'brand': ['Stark', 'Martell'],
    'category': ['Weapons', 'Weapons'],
    'supplier_name': ['Castle Black', 'Dorne'],
    'supplier_country': ['Westeros', 'Westeros'],
    'quantity': [1, 2],
    'unit_price': [500, 300],
    'discount': [0, 10],
    'revenue': [500, 590],
    'payment_type': ['Gold', 'Gold'],
    'sales_channel': ['Direct', 'Direct']
}

df = pd.DataFrame(data)

# Save to Excel buffer
buffer = io.BytesIO()
with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
    df.to_excel(writer, index=False)
buffer.seek(0)

# Send request
url = "http://localhost:8000/upload/sales"
files = {'file': ('test_upload.xlsx', buffer, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}

try:
    response = requests.post(url, files=files)
    with open("upload_log.txt", "w") as f:
        f.write(f"Status: {response.status_code}\n")
        f.write(f"Response: {response.text}\n")
    print(f"Status Code: {response.status_code}")
except Exception as e:
    with open("upload_log.txt", "w") as f:
        f.write(f"Error: {e}\n")
    print(f"Error: {e}")
