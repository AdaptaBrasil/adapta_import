from pandas import DataFrame
import os
import src.db as db

txt = ""

if __name__ == '__main__':
    df = db.dbThings.getSQLasDataframe("""
select g.geocod,g.name,g.state, i.id as idmaster,i2.id as iddetail,i.level,i2.level,c.geoobject_id,c.master_year,c.detail_year,value 
from adaptabrasil.indicator i 
 inner join adaptabrasil.indicator_indicator ii 
 on i.id = ii.indicator_id_master 
inner join adaptabrasil.indicator i2
on i2.id = ii.indicator_id_detail 
inner join adaptabrasil.contribution c 
on i.id = c.master_indicator_id 
and i2.id = c.detail_indicator_id
inner join adaptabrasil.geoobject g 
on c.geoobject_id = g.id
--where i2.level = i.level+1
--and value is not null
order by 3,4,1,2,5""")
    df.to_csv(r'd:\temp\proporcionalidades.csv',sep='|')
    master = ""
    head0 = ""
    head1 = ""
    for i,row in df.iterrows():
        if i == 0:
            if row['idmaster'] != master:
                master = row['idmaster']
                head0 += ':'+master
            head1 += row['idmaster']+'|'




