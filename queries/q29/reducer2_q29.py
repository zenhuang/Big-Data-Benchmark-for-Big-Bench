import sys

if __name__ == "__main__":
	
	current_key = None
	current_count = 0

	for line in sys.stdin:
		key, cat_id, cat, affine_cat_id, affine_cat, count = line.strip().split("\t")
  		try:
			count = int(count)
		except ValueError:
			continue
		if current_key == key:
        		current_count += count
		else:
        		if current_key:
	           		print "%s\t%s\t%s\t%s\t%s" % (cat_id, cat, affine_cat_id, affine_cat, current_count)
			current_count = count
        		current_key = key
	print "%s\t%s\t%s\t%s\t%s" % (cat_id, cat, affine_cat_id, affine_cat, current_count)
