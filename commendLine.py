import SQL

def main():
	first = True
	csvDB = SQL.csvDB()

	while 1:
		if first:
			print("== Welcome ==")
			first = False
		sql = input('>>>  ')
		if sql == 'q':
			quit = input("Quit? (Y/N)")
			if quit == 'Y' or quit == 'y':
				break
		else:
			csvDB.executeSQL(sql)

if __name__ == "__main__" :
	main()
	sql1 = "SELECT R.review_id, R.stars, R.useful FROM r.csv R WHERE R.stars >= 4 AND R.useful > 20"
	sql2 = "SELECT R1.user_id, R2.user_id, R1.stars, R2.stars FROM r.csv R1, r.csv R2 WHERE R1.stars = 5 AND R2.stars = 1 AND R1.useful > 50 AND R2.useful > 50 ON (R1.business_id = R2.business_id)"
	sql3 = "SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business.csv B, r.csv R WHERE B.city = 'Champaign' AND B.state = 'IL' ON (B.business_id = R.business_id)"
	sql4 = "SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business.csv B, r.csv R, photos.csv P WHERE B.state = 'IL' AND P = 'outside' AND R.useful > 20 ON (B.business_id = R.business_id, B.business_id = P.business_id)"
    