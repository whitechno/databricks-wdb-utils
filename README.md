# databricks-wdb-utils
Convenience utilities for use in Databricks.  
WDB stands for Whitechno-Databricks.

*Please note double `.scala` in file names!*  
This is due to the way Databricks imports notebooks from Source Files:  
it drops last `.scala` from a file name to create a notebook name.

#### To run:
```
%run ./Lib/WDB-Utils.scala
```
and then
```
import com.whitechno.databricks.utils.wdb._
```

### Databricks' guide to GitHub Version Control:

https://docs.databricks.com/user-guide/notebooks/github-version-control.html
