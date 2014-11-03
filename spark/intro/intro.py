textfile = sc.textFile("war_peace_text")
textfile.count()
textfile.first()
prince_lines = textfile.filter(lambda s: "Prince" in s)
prince_lines.count()

textfile.map(lambda s: len(s.split())).reduce(lambda a,b: a if a > b else b)
textfile.map(lambda s: len(s.split())).reduce(lambda a,b: a+b)
