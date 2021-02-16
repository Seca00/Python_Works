# -*- coding: utf-8 -*-
import pyclamd
import os
import gzip
import random
import struct
from Crypto.PublicKey import RSA
from Crypto.Cipher import AES
import subprocess

from logger import logger
import FileOperations
from ReadConf import *

'''
Checks for Black And White Lists, gets list from db by listType(w for Whitelist, b for Blacklist)
And checks if the file hash or extension is in the list,
if its in the list and the listType is b then dont send and move the file to Quarantine
If its in the list and the listType is w then send the file to the socket
If its not in the list and the listType is b then send the file to the socket
If its not in the list and the listType is w then dont send and move the file to Quarantine
'''
def listCheck(fileName, fileHash, listType):
	try:
		MODULE = "ListCheck"
		BWList = []
		extensions = []
		extensions = fileName.split(".")
		extensions.pop(0)
		List = getBWList(listType)
		Flag = 0
		if listType == "w":
			for val in extensions:
				if(val in List["ExtensionList"]):
					Flag = 1
			for val in List["HashList"]:
				if val == fileHash:
					Flag = 1
			if Flag == 1:
				return True
			elif Flag == 0:
				return False
		if listType == "b":
			for val in extensions:
				if(val in List["ExtensionList"]):
					Flag = 1
			for val in List["HashList"]:
				if val == fileHash:
					Flag = 1
			if Flag == 1:
				return False
			elif Flag == 0:
				return True
	except Exception as e:
		logger("Dosya icin liste kontrolu yapilirken bir hata olustu. %s (Hata:%s)" % (fileName, e) , MODULE)


def CompressFile(xmlDir):
	if getConf("IsCompressed", xmlDir) == "1":
		in_filename = getConf("FPath", xmlDir)
		with open(in_filename, 'rb') as inF:
			s = inF.read()
		fName = os.path.basename(in_filename) + '.gzip'
		out_filename = '/tmp/' + fName
		outF = gzip.GzipFile(out_filename, 'wb')
		outF.write(s)
		outF.close()
		setConf("FPath", out_filename, xmlDir)
	else:
		pass


#Creates encrypted file under '/tmp/'+[filename]+'.enc'
def EncryptFile(xmlDir, chunksize=64*1024):
	if getConf("encrypt") == "1":
		in_filename = getConf("FPath", xmlDir)
		out_filename = '/tmp/' + os.path.basename(in_filename) + '.enc'
		key = os.urandom(16)
		public_key_string = open("public_key.pem","r").read()
		public_key = RSA.importKey(public_key_string)
		enc_key = public_key.encrypt(key, 32)
		iv = ''.join(chr(random.randint(0, 0xFF)) for i in range(16))
		encryptor = AES.new(key, AES.MODE_CBC, iv)
		filesize = os.path.getsize(in_filename)

		with open(in_filename, 'rb') as infile:
			with open(out_filename, 'wb') as outfile:
				outfile.write(struct.pack('<Q', filesize))
				outfile.write(iv)
				while True:
					chunk = infile.read(chunksize)
					if len(chunk) == 0: 
						break
					elif len(chunk) % 16 != 0:
						chunk += ' ' * (16 - len(chunk) % 16)

					outfile.write(encryptor.encrypt(chunk))
		setConf("FPath", out_filename, xmlDir)
		setConf("Enckey", enc_key, xmlDir)
	else:
		pass

def getTags(path):
	tagCommands = getListConf("ruleList","TagCommands")
	tagResult = []
	for tagCommand in tagCommands:
		command = tagCommand.replace("#{$path}" , path)
		p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
		out, err = p.communicate()
		result = out.strip()
		tagResult.append(result)
	return tagResult

def CheckPriorityWithXml(priority):
	# Get the path of file xmls
	xmlDir = getConf("filexmlpath")

	# Convert type to the ones in the file xmls
	if priority["Type"] == "ext":
		priorityType = "FExt"
	elif priority["Type"] == "tag":
		priorityType = "Tags"
	# Loop through all the files in xml directories
	for fileName in os.listdir(xmlDir):
		filePath = os.path.join(xmlDir, fileName)

		# Read the file xml and get the needed priority feature.(Extension or Tag)
		filePriority = getListConf(filePath, priorityType)

		# If the file priority type is same with the priority value return this files xml. 
		# (E.g. File ext => "xml" And priority value = "xml")
		for fp in filePriority:
			if fp == priority["Value"]:
				return xmlDir





