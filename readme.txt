1. First install all dependencies
	Run: pip install -r requirements.txt
2. From terminal run:    (python main.py name/number/all)
3. Configure configuration.json for your purpose. Examples are in configuration.json


If you keep "bg_table_name" field in configuration.json empty ("") then all tables in BQ will be created by it's own name.

If you keep a name in "bg_table_name" (suppose "pubmed") then your all mesh and table will be added in a table names ("pubmed")

You can try both options to see what happens :) 



bq rm  -f -t poc_1.bq rm  -f -t poc_1.split133

bq rm  -f -t poc_1.bq rm  -f -t poc_1.split*

poc_1
auto_detect1
demo_nested
pubmed
pubmed_3_rows
pubmed_5
pubmed_5_v2
pubmed_mesh
pubmed_x_rows
small
small_mesh
bq rm  -f -t poc_1.split133
bq rm  -f -t poc_1.split133_mesh
bq rm  -f -t poc_1.split137
bq rm  -f -t poc_1.split137_mesh
bq rm  -f -t poc_1.split139
bq rm  -f -t poc_1.split139_mesh
bq rm  -f -t poc_1.split149
bq rm  -f -t poc_1.split149_mesh
bq rm  -f -t poc_1.split151
bq rm  -f -t poc_1.split151_mesh
bq rm  -f -t poc_1.split159
bq rm  -f -t poc_1.split159_mesh
bq rm  -f -t poc_1.split165
bq rm  -f -t poc_1.split165_mesh
bq rm  -f -t poc_1.split167
bq rm  -f -t poc_1.split167_mesh
bq rm  -f -t poc_1.split189
bq rm  -f -t poc_1.split189_mesh


bq rm  -f -t poc_1.auto_detect1
bq rm  -f -t poc_1.demo_nested
bq rm  -f -t poc_1.pubmed
bq rm  -f -t poc_1.pubmed_3_rows
bq rm  -f -t poc_1.pubmed_5
bq rm  -f -t poc_1.pubmed_5_v2
bq rm  -f -t poc_1.pubmed_mesh
bq rm  -f -t poc_1.pubmed_x_rows
bq rm  -f -t poc_1.small
bq rm  -f -t poc_1.small_mesh


