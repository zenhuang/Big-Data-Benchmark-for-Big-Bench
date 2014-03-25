import sys

def print_permutations(vals):
	l = len(vals)
	if l <= 1 or l*(l-1)/2 > 500:
		return

	vals.sort()
	for i in range(l-1):
		for j in range(i+1,l):
			print "%s\t%s" % (vals[i], vals[j])

	
	

if __name__ == "__main__":
	
	current_key = ''
	vals = []

	for line in sys.stdin:
		key, val = line.strip().split("\t")

		if current_key == '' :
			current_key = key
			vals.append(val)

		elif current_key == key:
			vals.append(val)

		elif current_key != key:
			print_permutations(vals)
			vals = []
			current_key = key
			vals.append(val)

	print_permutations(vals)
