#! /usr/bin/env python

#--------------------------------------------------------
# DBConsole.py
#
# This is runnable with Python v2.7.6
# Example command: python ./DBConsole.py < SOME_TEST_FILE
#
# @author Josh Xu
#--------------------------------------------------------
import sys


class DBConsole:
	'''
	Database console inplementation.

	The Console accepts the following commands:
		SET name value
			Set the variable name to the value value. 
			Neither variable names nor values will contain spaces.
		GET name
			Print out the value of the variable name, 
			or NULL if that variable is not set.
		UNSET name
			Unset the variable name, 
			making it just like that variable was never set.
		NUMEQUALTO value
			Print out the number of variables that are currently set to value. 
			If no variables equal that value, print 0.
		END
			Exit the program. 
			The program will always receive this as its last command.
	'''
	#--------------------------------------------------------
	# initialize the database console
	#--------------------------------------------------------
	def __init__(self):
		# initially, the end signal will be False
		# This flags if the Database consle is end of use.
		self.received_end_signal = False
		# initialize the database as empty
		self.db = {}
		# constatnts
		self.NULL_STRING = 'NULL'


	#--------------------------------------------------------
	# return true if the console received an end signal
	#--------------------------------------------------------
	def isEnded(self):
		return self.received_end_signal


	#--------------------------------------------------------
	# main entrance to process the commands from raw strings.
	# cmd_string is the raw strin going to process
	#--------------------------------------------------------
	def process_cmd_string(self, cmd_string):
		cmd_line = self.parse_cmd_string(cmd_string)
		if   cmd_line == []: # validate the input before real processing
			return
		elif cmd_line[0].upper() == 'SET':
			self.set(cmd_line[1:])
		elif cmd_line[0].upper() == 'GET':
			self.get(cmd_line[1:])
		elif cmd_line[0].upper() == 'UNSET':
			self.unset(cmd_line[1:])
		elif cmd_line[0].upper() == 'NUMEQUALTO':
			self.num_equal_to(cmd_line[1:])
		elif cmd_line[0].upper() == 'END':
			self.received_end_signal = True



	#--------------------------------------------------------
	# returns a list contains all the elements in a command.
	# cmd_string is the raw strin going to process
	# @returns a list contains all the elements in a command.
	#--------------------------------------------------------
	def parse_cmd_string(self, cmd_string):
		cmd_line = []
		cmd_line.extend(cmd_string.strip().split(' '))
		return cmd_line


	#--------------------------------------------------------
	# SET name value
	#--------------------------------------------------------
	def set(self, cmd_params):
		# validate the input before real processing
		if (len(cmd_params) != 2): 
			return
		else :
			self.db[cmd_params[0]] = cmd_params[1]


	#--------------------------------------------------------
	# GET name 
	#--------------------------------------------------------
	def get(self, cmd_params):
		# validate the input before real processing
		if (len(cmd_params) != 1): 
			return
		else :
			if self.db.has_key(cmd_params[0]):
				print self.db[cmd_params[0]]
			else :
				print self.NULL_STRING


	#--------------------------------------------------------
	# UNSET name 
	#--------------------------------------------------------
	def unset(self, cmd_params):
		# validate the input before real processing
		if (len(cmd_params) != 1): 
			return
		else :
			if self.db.has_key(cmd_params[0]):
				del self.db[cmd_params[0]]
			else :
				return


	#--------------------------------------------------------
	# NUMEQUALTO value
	#--------------------------------------------------------
	def num_equal_to(self, cmd_params):
		# validate the input before real processing
		if (len(cmd_params) != 1): 
			return
		else :
			print len(filter(lambda x: x == cmd_params[0], self.db.values()))




class LoggedDBConsole(DBConsole):
	'''
		DBConsole class with Transaction commands:

			BEGIN:
				Open a new transaction block. 
				Transaction blocks can be nested; 
				a BEGIN can be issued inside of an existing block.
			ROLLBACK:
				Undo all of the commands issued in the most recent transaction block, 
				and close the block. Print nothing if successful, 
				or print NO TRANSACTION if no transaction is in progress.
			COMMIT:
				Close all open transaction blocks, 
				permanently applying the changes made in them. 
				Print nothing if successful, 
				or print NO TRANSACTION if no transaction is in progress.
	'''
	def __init__(self):
		# initialize the database history as empty
		self.history = []
		# constatnts
		self.NO_TRANSACTION_STRING = 'NO TRANSACTION'
		# super(LoggedDBConsole, self).__init__()
		DBConsole.__init__(self)


	#--------------------------------------------------------
	# main entrance to process the commands from raw strings.
	# cmd_string is the raw strin going to process
	#--------------------------------------------------------
	def process_cmd_string(self, cmd_string):
		cmd_line = self.parse_cmd_string(cmd_string)
		if   cmd_line == []: # validate the input before real processing
			return
		elif cmd_line[0].upper() == 'BEGIN':
			self.beginTxn()
		elif cmd_line[0].upper() == 'ROLLBACK':
			self.rollBackTxn()
		elif cmd_line[0].upper() == 'COMMIT':
			self.commitTxn()

		# Same as the base console, with function logic changed
		elif cmd_line[0].upper() == 'SET':
			self.set(cmd_line[1:])
		elif cmd_line[0].upper() == 'GET':
			self.get(cmd_line[1:])
		elif cmd_line[0].upper() == 'UNSET':
			self.unset(cmd_line[1:])
		elif cmd_line[0].upper() == 'NUMEQUALTO':
			self.num_equal_to(cmd_line[1:])
		elif cmd_line[0].upper() == 'END':
			self.received_end_signal = True

	#--------------------------------------------------------
	# BEGIN
	# start a new Transaction in the history
	# this will add a new list to the history
	# 
	# Logic:
	# Open a new transaction block. 
	# Transaction blocks can be nested; 
	# a BEGIN can be issued inside of an existing block.
	#--------------------------------------------------------
	def beginTxn(self):
		# the 1st element is the latest Txn being operated.
		self.history.insert(0, {})


	#--------------------------------------------------------
	# ROLLBACK
	# delete the current Transaction opened in the history
	# 
	# Logic:
	# 	Undo all of the commands issued in the most recent transaction block, 
	# 	and close the block. Print nothing if successful, 
	#	or print NO TRANSACTION if no transaction is in progress.	
	#--------------------------------------------------------
	def rollBackTxn(self):
		if self.hasTxnInProgress():
			self.history =  self.history[1:]
		else :
			print self.NO_TRANSACTION_STRING


	#--------------------------------------------------------
	# COMMIT
	# Commit all the Transactions to db
	#
	# Logic:
	# Close all open transaction blocks, 
	# permanently applying the changes made in them. 
	# Print nothing if successful, 
	# or print NO TRANSACTION if no transaction is in progress.
	#--------------------------------------------------------
	def commitTxn(self):
		if self.hasTxnInProgress():
			# reverse the history to operate from the oldest Txn.
			for Txn in self.history[::-1]:
				self.db.update(Txn)
			self.history = []
		else :
			print self.NO_TRANSACTION_STRING


	#--------------------------------------------------------
	# return true if the history is not empty
	#--------------------------------------------------------
	def hasTxnInProgress(self):
		return len(self.history) > 0


	#--------------------------------------------------------
	# SET name value
	#--------------------------------------------------------
	def set(self, cmd_params):
		# validate the input before real processing
		if (len(cmd_params) != 2): 
			return
		else :
			if self.hasTxnInProgress():
				self.history[0][cmd_params[0]] = cmd_params[1]
			else :
				self.db[cmd_params[0]] = cmd_params[1]


	#--------------------------------------------------------
	# GET name 
	#--------------------------------------------------------
	def get(self, cmd_params):
		# validate the input before real processing
		if (len(cmd_params) != 1): 
			return
		else :
			if self.hasTxnInProgress():
				for Txn in self.history:
					if Txn.has_key(cmd_params[0]):
						print Txn[cmd_params[0]]
						return
			# super(LoggedDBConsole, self).get(cmd_params)
			DBConsole.get(self, cmd_params)


	#--------------------------------------------------------
	# UNSET name 
	#--------------------------------------------------------
	def unset(self, cmd_params):
		# validate the input before real processing
		if (len(cmd_params) != 1): 
			return
		else :
			if self.hasTxnInProgress():
				self.history[0][cmd_params[0]] = self.NULL_STRING
			else :
				# super(LoggedDBConsole, self).unset(cmd_params)
				DBConsole.unset(self, cmd_params)


	#--------------------------------------------------------
	# NUMEQUALTO value
	#--------------------------------------------------------
	def num_equal_to(self, cmd_params):
		# validate the input before real processing
		if (len(cmd_params) != 1): 
			return
		else :
			if self.hasTxnInProgress():
				db_copy = self.db.copy()
				# reverse the history to operate from the oldest Txn.
				for Txn in self.history[::-1]:
					db_copy.update(Txn)
				print len(filter(lambda x: x == cmd_params[0], db_copy.values()))
			else :
				print len(filter(lambda x: x == cmd_params[0], self.db.values()))



#--------------------------------------------------------
# main function defined here
#--------------------------------------------------------

# console = DBConsole()
console = LoggedDBConsole()

while (not console.isEnded()):
	cmd_string = sys.stdin.readline()
	# print cmd_string
	console.process_cmd_string(cmd_string)


