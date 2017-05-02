# databricks-wdb-utils
Convenience utilities for use in Databricks.  
WDB stands for Whitechno-Databricks.

#### All Databricks Notebooks source files are in `DBN` folder.
*Please note double `.scala` in file names!*  
This is due to the way Databricks imports notebooks from Source Files:  
it drops last `.scala` from a file name to create a notebook name.

#### To run:
```
%run ./WDB-Utils.scala
```
and then
```
import com.whitechno.databricks.utils.wdb._
```

### Databricks' guide to GitHub Version Control:

https://docs.databricks.com/user-guide/notebooks/github-version-control.html
