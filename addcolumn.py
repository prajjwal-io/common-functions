import pandas as pd

df = pd.read_csv('mycsv.csv')

stopwords = ['d.o.o', 'o.o.','plc', 'assn', 'ab', 'pllc', 'l.l.c.', 'pty', 'mfg', 'l.3.c.', 'private.', 'z.o.o.', 'pte.', 'co.,', 'lllp', 'kg', 'coop.', 'pvt.ltd', 'sp.k.', 'ltd./gte.', 'ltda', 's.cra.', 'bhd.', 'd/b/a', 'pte', 'lab,', 's.l.n.e.', 's.c.s', 'gesmbh', 'ag', 'l.p', 'v.o.s.','se', 'aps', 's.c.e', 'gmbh', 'private.ltd.', 'public', 'prof','low-profit', 'ltda.', '.co', 'pvt.', 'co', 'de', 'ent.', 'c.', 'sdn', 's.a.i.c.a.', 'corp', 'p.', 'd.n.o.', 'p', 'bv', 'limited.', 'ltée.', 'pted.', 'pty.', 's.coop', 'private', 'inc.,', 'r.l.', 'p/s', 'oü', 's.a.r.l', 'j.t.d', 'z', 'inc.', 'ltee', 's.p.a.', 'lab', 'incorp.', 'limited', 'incorp', 'ltd', 's.','prp', 'pmdn', 's/a', 'sa', 'limite', 'l.p.', 'dba:,', 'vfx', 's.c.s.', 'service.', 's.m.b.a.', 'p/l', 'corp.,', 'sp.', 's.c.a', 'e.v.', 'pvtltd', 'en', 'spol', 'pvtlimited', 'pvt', 'ptp', 'coop', 'k/s', 'inc', 'kom.', 's.r.o.', 'dba:', 'co.', 'intl', 'p.s.c.', 'llp', 'group.inc', 'ltd.,', 'limitée', 'ka/s', 'i/s', 'llc', 'private.ltd', 'ltée/ltd', 'ohg', 'holding', 'cvoa', 'pvt.ltd.', 'co,', 'a/s', 'a.', 'pted', 'cqb', 'ltée', 'ltd.', 'co-op', 'd.o.o.', 'a.m.b.a.', 'srk', 'sp.p.', 'comp', 'prp.', 'kol.', 'pts', 'bhd', 'sdn.']
def clear_stopwords(stopwords, name):
  try:
    stopwords = [x.lower() for x in stopwords]
    querywords = name.split()
    resultwords  = [word for word in querywords if word.lower() not in stopwords]
    result = ' '.join(resultwords)
  except Exception as e:
      return name        
  return result.replace(',','')
#print(stopwords)
print(clear_stopwords(stopwords , 'Academic Explorers, LLC'))

df['Company'] = df['name'].apply(lambda x : clear_stopwords(stopwords, x) )

df.to_csv('myswcsv.csv')