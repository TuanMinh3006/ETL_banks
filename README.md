# ETL_Banks
## Project ETL_Banks được chia làm 4 phần: Extract,Transform,Load,Log
## Mục đích:
Thu thập các thông tin vốn hóa theo thị trường của các ngân hàng lớn trên Thế giới và chuyển đổi số tiền vốn hóa(USD) thành các đồng tiền khác trên TG và lưu vào file .CSV và lưu vào database.
## Ngôn ngữ sử dụng:
+ Python
+ SQL
## Yêu cầu
+ Pandas
+ Beautiful Soup
+ datetime
+ request
+ sqlite3

## Phần 1: Extract
Mục đích: Thu thập thông tin (Tên, MC_USD_Billion) của Top 10 ngân hàng có vốn hóa thị trường lớn nhất Thế Giới

Input: https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks

![image](https://github.com/user-attachments/assets/902a3881-0034-4e88-9750-e653f0e361c2)

Output: Trả về 1 dataframe lưu Tên, MC_USD_Billion của Top 10 ngân hàng

![Task_2c_extract](https://github.com/user-attachments/assets/27689e66-4cf1-4b59-ab50-a52d60fc2191)


## Phần 2: Transform

Mục đích: Chuyển đổi từ USD thành EUR và INR(Rupee Ấn Độ) và GBP(Bảng Anh)

Input: DataFrame đã được thu thập từ phần extract

![Task_2c_extract](https://github.com/user-attachments/assets/27689e66-4cf1-4b59-ab50-a52d60fc2191)

Output: DataFrame đã được thêm cột 

Output: Trả về một dataframe gồm 4 cột : Name, MC_USD_Billion,MC_GBP_Billion,MC_EUR_Billion,MC_INR_Billion của top 10 ngân hàng
## Phần 3: Load

Mục đích: Load dataframe đã transform thành file .CSV và lưu vào database SQL có tên Banks.db 

Input: Dataframe đã được transform trên

OutPut: 

![image](https://github.com/user-attachments/assets/5a21a764-96b7-4752-99da-c3c40975d1c5)

Nội dung file .CSV

![image](https://github.com/user-attachments/assets/d6581851-e113-4adc-a027-bf4d38036ee2)

File .CSV và file database

## Phần 4: Log
Mục đích: Ghi lại các quá trình ETL nhằm quản lý thời gian và có thể nhận biết lỗi nếu xảy ra

OutPut: 

![image](https://github.com/user-attachments/assets/e575eae4-cc66-4f7e-9ce7-8d84a5ec9f3d)
