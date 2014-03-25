import sys

if __name__ == "__main__":
	
	for line in sys.stdin:
		key, val = line.strip().split("\t")
		print "%s\t%s" % (key, val)	
	
