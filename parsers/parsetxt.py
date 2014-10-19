import numpy
import scipy
import sys
import os
import re


#------------------------------------------------------------
def convertTXT(filename):


	#Find Page Files
	filelist = []
	path = os.getcwd() + '/' + filename + '/pages/'
	for f in os.listdir(path):
		if 'page' in f:
			newf = f
			if (len(f) == 10):
				newf = f[0:4] + '00' + f[4:]
			if (len(f) == 11):
				newf = f[0:4] + '0' + f[4:]
			filelist.append(path + newf)
	filelist.sort()
	print filelist
	"""
	#Loop through Pages
	print '[INFO] Readin Pages'


	#Open file and store text
	with open(filename + '/' + filename + '.corpus','w') as f:
		for i,infile in enumerate(filelist):

			#Verbose
			print '[INFO] Reading page ' + str(i+1)
			#infile = infile.replace('page00','page')
			#infile = infile.replace('page0','page')

			#Read
			content = ''
			with open(infile,'r') as instream:
				for line in instream:
					content = content + line
		
			#Remove line break
			text = content.replace('\n','')

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
			text = re.sub(r'-','', text)
			text = re.sub('\\((.*?)\\)','',text)
			text = re.sub(r'  ',' ', text)
			text = re.sub(r', ',' ', text)
			text = re.sub(r'[^a-z ]','', text)
			text = re.sub(r'  ',' ', text)

			#Write
			f.write(text + '\n')
			

			"""



#------------------------------------------------------------
if __name__ == '__main__':

	#Read and convert PDF to text
	os.system("clear")
	convertTXT(sys.argv[1])

