#!/usr/bin/php
<?php


function truncate($mysql, $db, $table)
{
 mysqli_select_db($mysql, $db);
 $sql = "truncate table {$db}.{$table};"; 
 $yresult = mysqli_query($mysql, $sql);
 if(!$yresult) 
   die("An error occurred.".mysqli_error($mysql)."\n");
 
 return true;
}

function getLastid($mysql, $db, $table)
{
 mysqli_select_db($mysql, $db);
 $sql = "select coalesce(max(dex_row_id), 0) last from $table;"; 
 $yresult = mysqli_query($mysql, $sql);
 if(!$yresult) 
   die("An error occurred.".mysqli_error($mysql)."\n");

 $row = mysqli_fetch_assoc($yresult);
 return $row["last"]; 
}

function getLastts($mysql, $db, $table)
{
 mysqli_select_db($mysql, $db);
 $sql = "select coalesce(max(dex_row_ts), '01/10/1970') last from $table;"; 
 $yresult = mysqli_query($mysql, $sql);
 if(!$yresult) 
   die("An error occurred.".mysqli_error($mysql)."\n");

 $row = mysqli_fetch_assoc($yresult);
 return $row["last"]; 
}


function xcopy($mssql, $mysql, $db, $table, $sql)
{
  $start = microtime(true);
  mysqli_select_db($mysql,  $db);
  mssql_select_db($db,   $mssql);
  
  $result = mssql_query($sql, $mssql, 20000);
  if($result === false)
   die("Error creating sync data\n");

  $s = 0;
  $r =  mssql_num_rows($result);

  $name_count  = mssql_num_fields($result);
  $name_list   = "";
  $update_list = "";

  $value_list  = "";
  $sql         = "";
  $radix       = 0;


  for ($i = 0; $i < $name_count; $i++) 
  {
     $x = strtolower(mssql_field_name($result, $i));  
     $name_list   .= "$x,";

     if($x != "dex_row_id")
       $update_list .= "$x = values($x),";
  }

  $name_list   = rtrim($name_list,   ",");
  $update_list = rtrim($update_list, ",");

  do
  {
    while ($row = mssql_fetch_row($result)) 
    {
       for ($i = 0; $i < $name_count; $i++)   
          $value_list .= "'".str_replace("'", "''",trim($row[$i]))."',";

      
        $value_list = rtrim($value_list, ",");      
        $radix ++;
        
        $sql .= "\n($value_list),";
        
        $value_list = "";

        if($radix > 2000)
        {
            
            $sql = trim($sql, ",");
            $sql = "insert into $table ($name_list) values $sql on duplicate key update $update_list;";

            $rset = mysqli_query($mysql, $sql);
            if($rset === false)
                die("Error inserting mysql data. \n".mysqli_error($mysql)."\n\n$sql\n\n");  
           
            $radix = 0;
            $sql   = "";
        }  
        $s ++;
    }
  }while(mssql_fetch_batch($result));


  if($sql != "")
  {
      $sql = trim($sql, ",");
      $sql = "insert into $table ($name_list) values $sql on duplicate key update $update_list;";

      $rset = mysqli_query($mysql, $sql);
      if($rset === false)
          die("Error inserting mysql data. \n".mysqli_error($mysql)."\n\n$sql\n\n");  
   }  
  $end = microtime(true); 
  $total = $end - $start;
  echo "imported $db.$table [ $s ] records in $total sec.\n";
}

function zcopy($table)
{
  global $config;

  $mssql = mssql_connect($config["mssql.host"], $config["mssql.user"], $config["mssql.pass"]); 
  if($mssql === false)
    die("Error connecting to GP database");

  $mysql = mysqli_connect($config["mysql.host"], $config["mysql.user"], $config["mysql.pass"]);
  if($mysql === false)
   die("Error connecting to mysql server. ".mysqli_connect_error());

  mysqli_set_charset($mysql, "utf8");   

  switch($table["type"])
  {
     case "truncate":
       truncate($mysql, $table["db"], $table["table"]);
       break;
     case "lastid":
       $x = getLastid($mysql, $table["db"], $table["table"]);
       $table["sql"] .= " where dex_row_id > '$x'";
       break;
     case "lastts":
       $x = getLastts($mysql, $table["db"], $table["table"]);
       $table["sql"] .= " where dex_row_ts > '$x'";
       break;
  }

  xcopy($mssql, $mysql, $table["db"], $table["table"], $table["sql"]);

  mysqli_close($mysql);
  mssql_close($mssql);
}


function preCook($sql)
{
   global $config;
   
   $start = microtime(true);
   $mysql = mysqli_connect($config["mysql.host"], $config["mysql.user"], $config["mysql.pass"], "zero");
   if($mysql === false)
    die("Error connecting to mysql server. ".mysqli_connect_error());

   mysqli_set_charset($mysql, "utf8");   
   
   $rset = mysqli_query($mysql, $sql);
   if($rset === false)
      die("Error updating mysql data. \n".mysqli_error($mysql)."\n\n$sql\n\n");             
   
   mysqli_close($mysql);
   $end = microtime(true);
   $total = $end - $start;
   echo "executed $sql in $total sec.\n";
}

$config["mssql.host"] = "192.168.0.25";
$config["mssql.user"] = "expedite";
$config["mssql.pass"] = "vagapvek";

$config["mysql.host"] = "localhost";
$config["mysql.user"] = "crusher";
$config["mysql.pass"] = "finni03";



$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "iv00101",  "sql" => "select ITEMNMBR, ITEMDESC, ITMTSHID, TAXOPTNS, ITMCLSCD, UOMSCHDL, USCATVLS_1, USCATVLS_2, USCATVLS_3, USCATVLS_4, USCATVLS_5, USCATVLS_6, INACTIVE, convert(varchaR(23),DEX_ROW_TS, 121) DEX_ROW_TS, DEX_ROW_ID from iv00101" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "iv00102",  "sql" => "select ITEMNMBR, LOCNCODE, QTYBKORD, QTYONORD, QTYONHND, ATYALLOC, DEX_ROW_ID from iv00102 where qtyonhnd != 0" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "iv10200",  "sql" => "select ITEMNMBR, TRXLOCTN, convert(varchar(23), DATERECD, 121) DATERECD, RCPTSOLD, QTYRECVD, QTYSOLD, RCPTNMBR, VENDORID, PORDNMBR, UNITCOST, DEX_ROW_ID from iv10200 where RCPTSOLD = 0" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "iv10401",  "sql" => "select PRCSHID, SEQNUMBR, EPITMTYP, ITEMNMBR, BRKPTPRC, ACTIVE, BASEUOFM, PRODTCOD, PROMOTYP, PROMOLVL, DEX_ROW_ID from iv10401 a where a.active = 1 and exists (select 1 from sop10110 c where c.active = 1 and a.prcshid = c.prcshid)" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "iv10402",  "sql" => "select PRCSHID, EPITMTYP, ITEMNMBR, UOFM, QTYFROM, QTYTO, PSITMVAL, EQUOMQTY, QTYBSUOM, SEQNUMBR, DEX_ROW_ID from iv10402 a where exists (select 1 from iv10401 b where b.active = 1 and a.prcshid = b.prcshid and a.itemnmbr = b.itemnmbr) and exists (select 1 from sop10110 c where c.active = 1 and a.prcshid = c.prcshid)" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "iv40201",  "sql" => "select UOMSCHDL, BASEUOFM, convert(varchaR(23),DEX_ROW_TS, 121) DEX_ROW_TS, DEX_ROW_ID from iv40201" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "iv40202",  "sql" => "select UOMSCHDL, UOFM, QTYBSUOM, convert(varchaR(23),DEX_ROW_TS, 121) DEX_ROW_TS, DEX_ROW_ID from iv40202" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "iv40700",  "sql" => "select LOCNCODE, LOCNDSCR, ADDRESS1, ADDRESS2, ADDRESS3, CITY, STATE, ZIPCODE, COUNTRY, DEX_ROW_ID from iv40700" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "iv40701",  "sql" => "select LOCNCODE, BIN, DEX_ROW_ID from iv40701" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "pm00200",  "sql" => "select VENDORID, VENDNAME, ADDRESS1, ADDRESS2, ADDRESS3, CITY, STATE, ZIPCODE, COUNTRY, PHNUMBR1, PHNUMBR2, VENDSTTS, CURNCYID, PYMTRMID, convert(varchaR(23),DEX_ROW_TS, 121) DEX_ROW_TS, DEX_ROW_ID from pm00200" ];
$tables[] = [ "type" => "lastid",   "db" => "cpj", "table" => "pop30300", "sql" => "select POPRCTNM, POPTYPE, VNDDOCNM, convert(varchaR(23), receiptdate, 121) receiptdate, VENDORID, VENDNAME, DEX_ROW_ID from pop30300" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "rm00101",  "sql" => "select CUSTNMBR, CUSTNAME, CUSTCLAS, TAXSCHID, ADDRESS1, ADDRESS2, ADDRESS3, COUNTRY, CITY, STATE, PHONE1, PHONE2, PHONE3, FAX, SLPRSNID, PYMTRMID, CRLMTAMT, CURNCYID, SALSTERR, HOLD, convert(varchaR(23),DEX_ROW_TS, 121) DEX_ROW_TS, DEX_ROW_ID from rm00101" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "rm00500",  "sql" => "select PRCSHID, PRODTCOD, LINKCODE, SEQNUMBR, PSSEQNUM, DEX_ROW_ID from rm00500 a where exists (select 1 from sop10110 c where c.active = 1 and a.prcshid = c.prcshid)" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "sop10100", "sql" => "select SOPNUMBE, convert(varchaR(23), DOCDATE, 121) DOCDATE, CUSTNMBR, LOCNCODE, USER2ENT, EXTDCOST, VOIDSTTS, convert(varchaR(23),DEX_ROW_TS, 121) DEX_ROW_TS, DEX_ROW_ID from sop10100" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "sop10106", "sql" => "select SOPNUMBE, USRDAT01, USRDAT02, USERDEF1, USERDEF2, USRDEF03, USRTAB01, USRTAB09, USRDEF04, DEX_ROW_ID from sop10106" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "sop10110", "sql" => "select PRCSHID, DESCEXPR, NTPRONLY, ACTIVE, convert(varchar(23), STRTDATE, 121) STRTDATE, convert(varchaR(23), ENDDATE, 121) ENDDATE, PROMO, CURNCYID, DEX_ROW_ID from sop10110 where active = 1" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "sop10200", "sql" => "select SOPNUMBE, LNITMSEQ, ITEMNMBR, ITEMDESC, UNITCOST, EXTDCOST, QUANTITY, UOFM, convert(varchaR(23),DEX_ROW_TS, 121) DEX_ROW_TS, DEX_ROW_ID from sop10200" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "tx00201",  "sql" => "select TAXDTLID, TXDTLDSC, TXDTLTYP, ACTINDX, TXDTLPCT, DEX_ROW_ID from tx00201" ];
$tables[] = [ "type" => "truncate", "db" => "cpj", "table" => "sy03100",  "sql" => "select CARDNAME, CKBKNUM1, DEX_ROW_ID from sy03100" ];

$tables[] = [ "type" => "truncate", "db" => "hsu", "table" => "iv00102",  "sql" => "select ITEMNMBR, LOCNCODE, QTYBKORD, QTYONORD, QTYONHND, ATYALLOC, DEX_ROW_ID from iv00102 where qtyonhnd != 0" ];
$tables[] = [ "type" => "truncate", "db" => "hsu", "table" => "iv40700",  "sql" => "select LOCNCODE, LOCNDSCR, ADDRESS1, ADDRESS2, ADDRESS3, CITY, STATE, ZIPCODE, COUNTRY, DEX_ROW_ID from iv40700" ];
$tables[] = [ "type" => "truncate", "db" => "hsu", "table" => "iv40701",  "sql" => "select LOCNCODE, BIN, DEX_ROW_ID from iv40701" ];
$tables[] = [ "type" => "truncate", "db" => "hsu", "table" => "iv10200",  "sql" => "select ITEMNMBR, TRXLOCTN, convert(varchar(23), DATERECD, 121) DATERECD, RCPTSOLD, QTYRECVD, QTYSOLD, RCPTNMBR, VENDORID, PORDNMBR, UNITCOST, DEX_ROW_ID from iv10200 where RCPTSOLD = 0" ];
$tables[] = [ "type" => "lastid",   "db" => "hsu", "table" => "pop30300", "sql" => "select POPRCTNM, POPTYPE, VNDDOCNM, convert(varchaR(23), receiptdate, 121) receiptdate, VENDORID, VENDNAME, DEX_ROW_ID from pop30300" ];

$tables[] = [ "type" => "truncate", "db" => "dynamics", "table" => "mc00100", "sql" => "select EXGTBLID, CURNCYID, convert(varchaR(23), EXCHDATE, 121) EXCHDATE, convert(varchaR(23), TIME1, 121) TIME1, XCHGRATE, convert(varchaR(23), EXPNDATE, 121) EXPNDATE, DEX_ROW_ID from mc00100" ];

$tables[] = [ "type" => "truncate", "db" => "expedite", "table" => "itemupc",     "sql" => "select upc, itemnmbr from itemupc" ];
$tables[] = [ "type" => "truncate", "db" => "expedite", "table" => "weight_cube", "sql" => "SELECT itemnmbr, cube, weight, min_wt, max_wt FROM weight_cube" ];

$tables[] = [ "type" => "truncate", "db" => "exp_report", "table" => "tb_container_item_tariffs",  "sql" => "SELECT itemnmbr, tariff FROM tb_container_item_tariffs" ];

$start = microtime(true);
$radix = 0;
foreach ($tables as $table) 
{
   $pid = pcntl_fork();
   if(!$pid)
   {
     zcopy($table);
     exit($radix);
   }

   $radix ++;
}

$radix ++;
echo "Waiting for [ $radix ] processes to complete\n\n";
while(pcntl_waitpid(0, $status) != -1)
{
  #$status = pcntl_wexitstatus($status);
  #echo $tables[$status]["db"].".".$tables[$status]["table"]." exit()ed\n";
}


$tables = [
            "call zero.updateproducts()",
            "call zero.updatevendors()",
            "call zero.updateimportvendors()",
            "call zero.updateimportsites()",
            "call zero.updatepackages()",
            "call zero.updatepackagedetails()",
            "call zero.updatepackagedetails()",
            "call zero.updatepricesheets()",
            "call zero.updatepricesheetdetails()",
            "call zero.updateproductquantities()",
            "call zero.updatecustomers()",
            "call zero.updatetaxes()", 
            "call zero.updateitemupc()",
            "call zero.updateexchangerates()"
          ];


$radix = 0;
foreach ($tables as $update) 
{
   $pid = pcntl_fork();
   if(!$pid)
   {
     preCook($update);
     exit($radix);
   }
   $radix ++;
}

$radix ++;
echo "\n\nWaiting for [ $radix ] chefs to complete\n\n";
while(pcntl_waitpid(0, $status) != -1)
{
  #$status = pcntl_wexitstatus($status);
}

/*
$mysql = mysqli_connect($config["mysql.host"], $config["mysql.user"], $config["mysql.pass"], "zero");
 if($mysql === false)
  die("Error connecting to mysql server. ".mysqli_connect_error());

 mysqli_set_charset($mysql, "utf8");   

 foreach ($tables as $sql) 
 {
   if(!empty($sql))
   {
     $rset = mysqli_query($mysql, $sql);
     if($rset === false)
        die("Error inserting mysql data. \n".mysqli_error($mysql)."\n\n$sql\n\n");             
   }
 }
 mysqli_close($mysql);
*/

$end = microtime(true);

$total = $end - $start;
echo "\n\nDone. $total Seconds\n\n\n";
?>

