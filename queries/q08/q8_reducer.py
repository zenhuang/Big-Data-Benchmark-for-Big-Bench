import sys

def npath(vals):
	vals.sort()
	ready = 0
	for val in vals:
		if ready == 0 and val[5] == 'review':
			ready = 1
			continue
		if ready == 1 and val[4] != 'NULL':
			print "%s\t%s" % (str(val[0]), val[4])
			ready = 0

if __name__ == "__main__":
	
	current_key = ''
	vals = []

	for line in sys.stdin:
		val1, key, val2, val3, val4, val5 = line.strip().split("\t")
		int(val2), int(val3), val1, key, val4, val5
		if current_key == '' :
			current_key = key
			vals.append((int(val2), int(val3), val1, key, val4, val5))

		elif current_key == key:
			vals.append((int(val2), int(val3), val1, key, val4, val5))

		elif current_key != key:
			npath(vals)
			vals = []
			current_key = key
			vals.append((int(val2), int(val3), val1, key, val4, val5))

	npath(vals)	
	
