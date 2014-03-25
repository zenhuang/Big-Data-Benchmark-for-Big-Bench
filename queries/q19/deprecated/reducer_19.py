import sys
	

if __name__ == "__main__":
	
	for line in sys.stdin:
		item, polarity = line.strip().split("\t")
		if polarity is not "neutral":
			print("%s\t%s") % (item, polarity)
	