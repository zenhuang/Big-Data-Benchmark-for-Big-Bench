import sys

if __name__ == "__main__":
	
	for line in sys.stdin:
		val1, key, val2, val3, val4 = line.strip().split("\t")
		print "%s\t%s\t%s\t%s\t%s" % (val1, key, val2, val3, val4)	
	
