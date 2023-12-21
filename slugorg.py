from slugify import slugify

texts = ["Delta Airline","Sioux Geway Col. Bud Day Field","M. R. Štefánik" , "ZZI.net" , "Zz-capital Talent Partners, Inc.", "ZypMedia, Inc.", "Zynom Technologies Pvt Ltd" , "Zzzzapp Wireless ltd." , "Zuma | Azumi ltd." , "Zulily: Shop All The Things!" , "ZT Bechtol, M.D. ,PLLC", "Zoya#039;s salon and spa", "Zoomot.com(coolshare)"]


for txt in texts:
    r = slugify(txt)
    print(f" {txt} : { r} \n") 
    

    