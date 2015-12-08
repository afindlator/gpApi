#!/usr/bin/php
<?php

function sanitize($data)
{
 return str_replace("'", "''", trim($data));
}


function copy_table_structure($db, $table, $keys)
{
global $con;
global $gp;

$unique  = "";
$primary = "";
$index   = "";


$xsql = "";
$sql = "";
$sql = "
        select 
         COLUMN_NAME,
         case 
          when DATA_TYPE in ('char', 'varchar', 'nchar', 'nvarchar') then DATA_TYPE + ' (' + cast(CHARACTER_MAXIMUM_LENGTH as varchar(10)) + ')'
          when DATA_TYPE in ('numeric')                              then DATA_TYPE + ' (' + cast(NUMERIC_PRECISION as varchar(10)) + ', ' + cast(NUMERIC_SCALE as varchar(10)) + ')'    
          when DATA_TYPE in ('tinyint')                              then 'smallint'
          when DATA_TYPE in ('datetime')                             then 'datetime(3)'    
          when DATA_TYPE in ('binary')                               then 'blob'
          else cast(DATA_TYPE as varchar(20)) 
         end as DATA_TYPE 
         FROM INFORMATION_SCHEMA.COLUMNS 
         where TABLE_NAME = '$table';
       ";


$xresult = mssql_query($sql, $gp, 2000);
do 
{

 while($line = mssql_fetch_assoc($xresult))
 {
  if($xsql == "")
   $xsql .= trim($line["COLUMN_NAME"])." ".trim($line["DATA_TYPE"]);
  else
   $xsql .= ", ".trim($line["COLUMN_NAME"])." ".trim($line["DATA_TYPE"]); 
 }

 $batchsize = mssql_fetch_batch($xresult); 
}while($batchsize);

mssql_free_result($xresult);

$primary = !empty($keys["primary"]) ? ", primary key(".$keys["primary"].")" : "";

if(!empty($keys["index"]))
{
   if(is_array($keys["index"]))
   {
      foreach ($keys["index"] as $value) 
      {
         $index .= ", index($value)";
      }
   }
   else
   {
      $index .= ", index(".$keys["index"].")";
   }
}

if(!empty($keys["unique"]))
{
   if(is_array($keys["unique"]))
   {
      foreach ($keys["unique"] as $value) 
      {
         $unique .= ", unique($value)";
      }
   }
   else
   {
      $unique .= ", unique(".$keys["unique"].")";
   }
}

echo "Creating table {$db}_$table\n";
$qsql = "create table {$db}_$table($xsql $primary $index $unique);";
$yresult = mysqli_query($con, $qsql);
if(!$yresult) 
{
  die("An error occurred.\n".mysqli_error($con)."\n$qsql");
}
       
}


function table_exists($db, $table_name)
{
 global $con;

 $sql = "SELECT count(table_name) as cnt FROM information_schema.tables WHERE table_name='{$db}_".strtolower($table_name)."';"; 
 $yresult = mysqli_query($con, $sql);
 if(!$yresult) 
 {
   echo "An error occurred.".mysqli_error($con)."\n";
 }

 $row = mysqli_fetch_assoc($yresult);
 if($row["cnt"] == 0)
  return false;
 else 
  return true; 
 
}

function copy_table_data($db, $table, $keys, $filter)
{
 global $gp;
 global $con;
 
 /*
 if($db == "dynamics")
  mssql_select_db("dynamics");
 else 
  mssql_select_db("cpj");
  */

  mssql_select_db($db);
  
 echo "Pulling data for table {$db}_$table...\n";
 $table  = strtolower($table);
 $filter = strtolower($filter);

 $field_names  = "";
 $field_values = "";
 
 #$key_id = 0;
 
 $xkeys = !empty($keys["primary"]) ? explode(", ", strtolower($keys["primary"])) : [];
 $primary = [];

 foreach ($xkeys as $value) 
    $primary[$value] = -1;
 
 if(!table_exists($db, $table))
 {
  echo "Table {$db}_$table does not exist.\nCopying table structure.\n";
  copy_table_structure($db, $table, $keys);
 }


 $result = mysqli_query($con, "select coalesce(max(modified), '01/10/1970') modified from sync_log where db = '$db' and tbl = '$table';");
 if($result === false)
    die("Error can not get last modified date. ".mysqli_error($con));  

 $row = mysqli_fetch_assoc($result);
 $mysql_modified = $row["modified"];
 mysqli_free_result($result);

 $result = mssql_query("select convert(varchar(23), modify_date, 121) modify_date from {$db}.sys.tables where name = '$table';", $gp);
 if($result === false)
    die("Error can not get last modified date. ".mssql_get_last_message());  

 $row = mssql_fetch_assoc($result);
 $mssql_modified = $row["modify_date"];
 mssql_free_result($result);

 echo "\n\n\n######################\n\n$mssql_modified == $mysql_modified\n\n#######################\n\n\n";
 if($mssql_modified == $mysql_modified)
  return;

 $where = ""; 
 switch($filter)
 {
    case "dex_row_id":
      $result = mysqli_query($con, "select coalesce(max(dex_row_id), 0) modified from {$db}_$table;");
      if($result === false)
        die("Error can not get last modified date. ".mysqli_error($con));  

      $row = mysqli_fetch_assoc($result);
      $where = " where dex_row_id > '".$row["modified"]."'";
      mysqli_free_result($result);
      break;
    case "dex_row_ts":
      $result = mysqli_query($con, "select coalesce(max(dex_row_ts), '01/10/1970') modified from {$db}_$table;");
      if($result === false)
        die("Error can not get last modified date. ".mysqli_error($con));  

      $row = mysqli_fetch_assoc($result);
      $where = " where dex_row_ts > '".$row["modified"]."'";
      mysqli_free_result($result);
      break;
    case "active":
      $where = " where active = 1";
      mysqli_query($con, utf8_encode("truncate table {$db}_$table;")); 
      break;
    default:
      mysqli_query($con, utf8_encode("truncate table {$db}_$table;")); 
      break;
 }

 $sql = "select * from $table $where";
 $rset = mssql_query($sql, $gp, 5000);
 if(!$rset) 
    die('MSSQL error: ' . mssql_get_last_message()."\n\n $sql \n\n\n");
 
 $cnt  = mssql_num_fields($rset);

 for($inc=0;$inc<$cnt;$inc++)
 {
   if($field_names == "")
    $field_names  = mssql_field_name($rset, $inc);
   else
    $field_names .= ", ".mssql_field_name($rset, $inc);
 
  $ykey = trim(strtolower(mssql_field_name($rset, $inc)));

  if(isset($primary[$ykey]))
    $primary[$ykey] = $inc;
 }

 $zkey = "";
 foreach ($primary as $key => $value) 
 {
   if($zkey != "")
      $zkey .= ":$key";
   else
      $zkey .= "$key"; 
 }

 $p_sql = "";
 $radix =  0;
 $counter = 0;
 $xdata = "";
 echo "\n\n";
 do
 {
  while($line = mssql_fetch_row($rset))
  {
   $field_values = "";
   
   $vkey = "";
   foreach ($primary as $key => $value) 
   {
     if($vkey != "")
        $vkey .= ":".$line[$value];
     else
        $vkey .= $line[$value]; 
   }


   echo "Importing ($zkey ".$vkey.")\r";
   
   for($inc=0;$inc<$cnt;$inc++)
   {
    
    if(mssql_field_type($rset, $inc) == "datetime")
     $xdata  = "'".date(DATE_ATOM, strtotime($line[$inc]))."'";
    else
     $xdata  = "'".sanitize($line[$inc])."'" ;
   
    if($field_values == "")
     $field_values  = "$xdata";
    else
     $field_values .= ", $xdata";  

   }
   
   if($p_sql != "")
     $p_sql .= ", ($field_values)";
   else
     $p_sql .= "($field_values)";

   if($radix == 500)
   {
      mysqli_query($con, utf8_encode("insert into {$db}_$table ($field_names) values $p_sql;"));
      $radix =  0;
      $p_sql = "";
   }
   
   $radix ++;
   $counter ++;
   #echo "$p_sql\n";         
   #mysqli_query($con, utf8_encode($p_sql));
  }
 }while(mssql_fetch_batch($rset));

 if($p_sql != "")
 {
    mysqli_query($con, utf8_encode("insert into {$db}_$table ($field_names) values $p_sql;"));
    $radix =  0;
    $p_sql = "";
 }
 
 $p_sql = "insert into sync_log (db, tbl, modified, synced, rows) values ('$db', '$table', '$mssql_modified', now(), '$counter');";
 mysqli_query($con, utf8_encode($p_sql));

 echo "\n\nTable {$db}_$table completed.\n\n";
}

date_default_timezone_set("america/jamaica");


$gp = mssql_connect("192.168.0.25", "expedite", "vagapvek"); 
if($gp === false)
  die("Error connecting to mssql server. ".mssql_get_last_message());

$con = mysqli_connect("localhost", "crusher", "finni03", "devop");
if($con === false)
  die("Error connecting to mysql server. ".mysqli_connect_error());

mysqli_set_charset($con, "utf8");

/*
if($argc < 4)
 echo "Not enough parameters";

copy_table_data($argv[1], $argv[2], $argv[3]);
*/

#copy_table_data("CPJ", "iv00101", [ "primary" => "DEX_ROW_ID", "unique" => "itemnmbr" ], "DEX_ROW_TS");
copy_table_data("CPJ", "sop30300", [ "primary" => "DEX_ROW_ID", "unique" => "soptype, sopnumbe, lnitmseq" ], "DEX_ROW_TS");






/*
copy_table_data "CPJ",      "CM00100", "CHEKBKID");
copy_table_data "CPJ",      "PM00100", "VNDCLSID");
copy_table_data "CPJ",      "SY00600", "CMPANYID");
copy_table_data "CPJ",      "SY03100", "PYBLGRBX");
copy_table_data "CPJ",      "SY00300", "SGMTNUMB");
copy_table_data "CPJ",      "SY03300", "PYMTRMID");
copy_table_data "CPJ",      "SY03000", "SHIPMTHD");
copy_table_data "CPJ",      "SY03300", "PYMTRMID");
copy_table_data "CPJ",      "TX00101", "TAXSCHID");
copy_table_data "CPJ",      "TX00102", "TAXSCHID");
copy_table_data "CPJ",      "TX00201", "TAXDTLID");
copy_table_data "CPJ",      "GL00100", "accatnum");
copy_table_data "CPJ",      "GL00102", "accatnum");
copy_table_data "dynamics", "MC00100", "EXGTBLID");
copy_table_data "CPJ",      "MC00200", "actindx");
copy_table_data "CPJ",      "PM00200", "VENDORID");
copy_table_data "CPJ",      "IV40600", "uscatval");
copy_table_data "CPJ"       "IV40201", "UOMSCHDL");
copy_table_data "CPJ"       "IV40202", "UOMSCHDL");
copy_table_data "CPJ"       "IV40400", "ITMCLSCD");
copy_table_data "CPJ"       "IV40700", "LOCNCODE");
copy_table_data "CPJ"       "IV40701", "BIN");
copy_table_data "CPJ"       "IV00117", "itemnmbr"); 
copy_table_data "CPJ"       "IV00101", "itemnmbr");
copy_table_data "CPJ"       "RM00201", "CLASSID");
copy_table_data "CPJ"       "RM00303", "SALSTERR");
copy_table_data "CPJ"       "RM00301", "SLPRSNID");
copy_table_data "CPJ"       "RM00101", "CUSTNMBR");  
copy_table_data "CPJ"       "RM00102", "CUSTNMBR");  

create table sync_log(id int not null auto_increment, db varchar(32) not null, tbl varchar(32) not null, modified timestamp(3) not null, synced timestamp(3) not null, primary key(id), rows int not null);


*/


?>
