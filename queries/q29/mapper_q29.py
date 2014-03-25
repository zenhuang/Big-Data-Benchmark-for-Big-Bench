import sys

if __name__ == "__main__":
	
	for line in sys.stdin:
		val1, val2, val3 = line.strip().split("\t")
		print "%s\t%s\t%s" % (val1, val2, val3)	
	
