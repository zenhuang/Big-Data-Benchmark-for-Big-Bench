import sys

if __name__ == "__main__":
	
	for line in sys.stdin:
		val1, val2, val3, val4 = line.strip().split("\t")
		key = val1 + ":" + val3
		print "%s\t%s\t%s\t%s\t%s\t%s" % (key, val1, val2, val3, val4, 1)	
	
