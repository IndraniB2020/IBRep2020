import pandas as pd
import numpy as np
import PyPDF2
from tabula.io import read_pdf
from tabula import read_pdf
import pdfplumber

#print("ttest")
#2023 02 28b Provincial Wastewater Trendlines for All Regions
# df = PyPDF2.PdfReader('2023 02 28b Provincial Wastewater Trendlines for All Regions.pdf')
# print(len(df.pages))

# page = df.pages[1]
# count = 0

# for image_file_object in page.images:
#     with open(str(count) + image_file_object.name, "wb")
#  as fp:
#         fp.write(image_file_object.data)
#         count += 1
# print(page.extract_text((0, 1000)))

# convert PDF into CSV
# Read a PDF File
# file_path = "2023 02 28b Provincial Wastewater Trendlines for All Regions.pdf"
# df = read_pdf(file_path)
# tabula.to_csv("testtry.csv")

# #df = tabula.io.read_pdf("2023 02 28b Provincial Wastewater Trendlines for All Regions.pdf", pages='all')[0]
# #tabula.convert_into(df, "ww.csv", output_format="csv", pages='all')
# print(df)

pdf = pdfplumber.open('2023 02 28b Provincial Wastewater Trendlines for All Regions.pdf')
#page = pdf.pages[0]
#text = page.extract_table()
#text = page.extract_text()
line = []
# text.to_csv("testww.csv")
# print("file created")
for page in pdf.pages:
    text = page.extract_table()
    #print("page---",page, text)
    line += text
    df = pd.DataFrame(line)
    df.to_csv('rawtestwwscrape.csv')

########################################
import fitz

filename = "2023 02 28b Provincial Wastewater Trendlines for All Regions.pdf"
file = fitz.open(filename)
image_list = file.getPageImageList(0)
print(image_list)




print("file created")


