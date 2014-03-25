import sys

if __name__ == "__main__":
	
	for line in sys.stdin:
		item, review = line.strip().split(",")
		polarity = sentiment_analysis(review)
		print "%s\t%s" % (item, polarity)	
	