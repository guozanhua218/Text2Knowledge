import PyPDF2 as pdf
import numpy
import scipy
import sys
import os
import re


#------------------------------------------------------------
def convertPDF(filename):


	#Create Object
	print '[INFO] Reading PDF'
	pdfobject = pdf.PdfFileReader(file('../books/' + filename + '/' + filename + '.pdf', "rb"))
	nPages = pdfobject.getNumPages()


	#Open file and store text
	with open('../books/' + filename + '/' + filename + '.corpus','w') as f:
		for i in range(0,816):

			#Extract text
			print '[INFO] Reading page ' + str(i+1)
			thispage = pdfobject.getPage(i)
			text = thispage.extractText()

			#Add spacing between lower and upper case letters
			matches = re.findall(r'[a-z][A-Z]',text)
			for match in matches:
				replacer = match[0] + ' ' + match[1]
				text = text.replace(match,replacer)
			matches = re.findall(r'[a-z].[A-Z]',text)
			for match in matches:
				replacer = match[0] + '. ' + match[2]
				text = text.replace(match,replacer)

			#Remove other characters
			text = text.lower()
			text = re.sub(r'!', 'fi',text)
			text = re.sub(r'"', 'fl',text)
			text = re.sub(r'-','', text)
			text = re.sub('\\((.*?)\\)','',text)
			text = re.sub(r'e3e','eje', text)
			text = re.sub(r'  ',' ', text)
			text = re.sub(r', ',' ', text)
			text = re.sub(r'[^a-z ]','', text)
			text = re.sub(r'  ',' ', text)

			#Write
			f.write(text + '\n')






#------------------------------------------------------------
if __name__ == '__main__':

	#Read and convert PDF to text
	os.system("clear")
	convertPDF(sys.argv[1])

