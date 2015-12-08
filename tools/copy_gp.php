 <?php




function econnect_copy($sql, $key, $node)
{
global $old_gp;
global $new_gp;

$name = "";
$inc  = 0;
$break = 0;
$st_proc = "";

$rset = mssql_query($sql, $old_gp);
while($line = mssql_fetch_assoc($rset))
{
 $st_proc = "";
 $break = 0;
 for($inc=0; $inc < mssql_num_fields($rset); $inc++, $break++)
 {
  $name = trim(mssql_field_name($rset, $inc));
  if($break > 1)
  {
    $st_proc .= "\n   ";
    $break = 0;
  }
  $st_proc .= "@I_v$name='" . str_replace("'", "''", trim($line[$name])) . "', ";
 }


 $st_proc = "
 declare @O_iErrorState int; declare @oErrString varchar(255); 
 select @O_iErrorState = 0;
 exec $node\n   $st_proc @oErrString = @oErrString output,@O_iErrorState = @O_iErrorState output;
 select @O_iErrorState as ret_val, ErrorDesc from  DYNAMICS..taErrorCode where errorcode = @O_iErrorState;
 "; 

 #PHP bug fix work around
 $st_proc = str_replace("@I_vUnrealized_Purchase_Price_Vari=", "@I_vUnrealized_Purchase_Price_Variance_Acct=", $st_proc);

 echo "$node " . trim($line[$key]) . ". ";
 $rset_x = mssql_query($st_proc, $new_gp);
 $line_x = mssql_fetch_assoc($rset_x);
   
 if($line_x["ret_val"] == 0)
 {
  echo "Successful.\n";
 } 
 else
 {
  echo "Failed. Reason: " .  trim($line_x["ErrorDesc"]) . "\n$st_proc\n";
  #exit();
 }
     
 mssql_free_result($rset_x);
}

}



function econnect_echo($sql, $key, $node)
{
global $old_gp;
global $new_gp;


$name = "";
$inc  = 0;
$break = 0;
$st_proc = "";



echo "
declare @x_factor varchar(255);
declare @O_iErrorState int; declare @oErrString varchar(255);  

select @O_iErrorState = 0
";

$rset = mssql_query($sql, $old_gp);
while($line = mssql_fetch_assoc($rset))
{
 $st_proc = "";
 $break = 0;
 for($inc=0; $inc < mssql_num_fields($rset); $inc++, $break++)
 {
  $name = trim(mssql_field_name($rset, $inc));
  if($break > 5)
  {
    $st_proc .= "\n   ";
    $break = 0;
  }
  $st_proc .= "@I_v$name='" . str_replace("'", "''", trim($line[$name])) . "', ";
 }


$st_proc = " 
if (@O_iErrorState = 0)
 begin
  select @x_factor = '" . trim($line[$key]) . "';
  exec $node\n   $st_proc @oErrString = @oErrString output,@O_iErrorState = @O_iErrorState output; 
 end
  
 ";
 
echo $st_proc;
}

echo "select @x_factor as x, @O_iErrorState as ret_val, ErrorDesc from  DYNAMICS..taErrorCode where errorcode = @O_iErrorState;";

}



function table_copy($xsql, $table)
{
global $old_gp;
global $new_gp;


$name    = "";
$names   = "";
$feilds  = "";
$sql     = "truncate table $table;";

$inc  = 0;

$rset = mssql_query($xsql, $old_gp);
while($line = mssql_fetch_assoc($rset))
{
 $names="";
 $feilds = "";

 for($inc=0; $inc < mssql_num_fields($rset); $inc++)
 {
  $name = trim(mssql_field_name($rset, $inc));
 
  if($names == "")
    $names = $name;
  else
    $names .= ", $name";
    
    
  if($feilds == "")
    $feilds = "'".str_replace("'", "''", trim($line[$name]))."'";
  else
    $feilds .= ", '".str_replace("'", "''", trim($line[$name]))."'";
 }

 $sql .= "insert into $table ($names) values ($feilds);\n"; 
 echo "$sql";
 mssql_query($sql, $new_gp);
 $sql = "";
 
}

//echo $sql;


}


/*
function copy_journals($sql, $batch)
{
global $old_gp;
global $new_gp; 


$line_seq = 16384;
$last_je = "";

$rset = mssql_query($sql, $old_gp, 1000);
$intNumRows = mssql_num_rows($rset);
while ($intNumRows > 0) 
{
 while($line = mssql_fetch_assoc($rset))
 {

 if($last_je != trim($line["JRNENTRY"]))
 {
  if($last_je != "")
  {
   $xsql = "
   begin transaction
   set nocount on
   declare @O_iErrorState int
   declare @oErrString    varchar(255) 

   select @O_iErrorState = 0, @oErrString = '';

   $header
   $detail

   if (@O_iErrorState = 0)
   begin
    exec taCreateUpdateBatchHeaderRcd
     @I_vBACHNUMB='$batch', @I_vSERIES='2', @I_vBCHSOURC='0', @I_vDOCAMT='0', @I_vORIGIN='1', @I_vNUMOFTRX='0', 
     @oErrString = @oErrString output, @O_iErrorState = @O_iErrorState output
   end

   if @O_iErrorState = 0
    begin
      commit transaction
      select 'OK' as res;
    end
   else
    begin
      rollback transaction
      select 'ERR' as res, rtrim(ErrorDesc) as message from DYNAMICS..taErrorCode where errorcode=@O_iErrorState;
    end
   ";

   $xrset = mssql_query($xsql, $new_gp);
   $xline = mssql_fetch_assoc($xrset);
   if($line)
   {
    if($xline["res"] == "OK")
     echo "Imported journal: $last_je, Batch: $batch\n";
    else
     echo "Failed to imported journal: $last_je, Batch: $batch. Reason: " . trim($xline["message"]) . "\n$xsql\n\n";
     
    mssql_free_result($xrset);
   }
   else
   {
    echo "Failed to imported journal (php): $last_je, Batch: $batch. Reason: " . mssql_get_last_message() . "\n$xsql\n\n";
   }
  }
 
  $line_seq = 16384;
  $last_je = trim($line["JRNENTRY"]);
  $detail = "";
  $header = "
  if (@O_iErrorState = 0)
   begin
    exec taGLTransactionHeaderInsert
       @I_vBACHNUMB = '$batch', 
       @I_vJRNENTRY = '" . trim($line["JRNENTRY"]) . "', 
       @I_vREFRENCE = '" . str_replace("'", "''", trim($line["REFRENCE"])) . "', 
       @I_vTRXTYPE  = '0', 
       @I_vTRXDATE  = '" . trim($line["TRXDATE"])  . "', 
       @I_vSERIES   = '" . trim($line["SERIES"])   . "', 
       @I_vCURNCYID = '" . trim($line["CURNCYID"]) . "', 
       @I_vXCHGRATE = '" . trim($line["XCHGRATE"]) . "', 
       @I_vRATETPID = '" . trim($line["RATETPID"]) . "', 
       @I_vEXCHDATE = '" . trim($line["EXCHDATE"]) . "', 
       @I_vTIME1    = '" . trim($line["TIME1"])    . "', 
       @I_vSOURCDOC = '" . trim($line["SOURCDOC"]) . "', 
       @oErrString  = @oErrString output,@O_iErrorState = @O_iErrorState output
   end
  ";
 }

 $detail .= "
 if (@O_iErrorState = 0)
  begin
   exec taGLTransactionLineInsert
      @I_vBACHNUMB   = '$batch', 
      @I_vSQNCLINE   = '" . $line_seq                 . "', 
      @I_vACTNUMST   = '" . trim($line["ACTNUMST"])   . "', 
      @I_vJRNENTRY   = '" . trim($line["JRNENTRY"])   . "', 
      @I_vCRDTAMNT   = '" . trim($line["CRDTAMNT"])   . "', 
      @I_vDEBITAMT   = '" . trim($line["DEBITAMT"])   . "', 
      @I_vDSCRIPTN   = '" . str_replace("'", "''", trim($line["DSCRIPTN"]))   . "', 
      @I_vORCTRNUM   = '" . trim($line["ORCTRNUM"])   . "', 
      @I_vORDOCNUM   = '" . str_replace("'", "''", trim($line["ORDOCNUM"]))   . "', 
      @I_vORMSTRID   = '" . str_replace("'", "''", trim($line["ORMSTRID"]))   . "', 
      @I_vORMSTRNM   = '" . str_replace("'", "''", trim($line["ORMSTRNM"]))   . "', 
      @I_vORTRXTYP   = '" . trim($line["ORTRXTYP"])   . "', 
      @I_vOrigSeqNum = '" . trim($line["OrigSeqNum"]) . "', 
      @I_vDOCDATE    = '" . trim($line["DOCDATE"])    . "', 
      @I_vCURNCYID   = '" . trim($line["CURNCYID"])   . "', 
      @I_vXCHGRATE   = '" . trim($line["XCHGRATE"])   . "', 
      @I_vRATETPID   = '" . trim($line["RATETPID"])   . "', 
      @I_vEXCHDATE   = '" . trim($line["EXCHDATE"])   . "', 
      @I_vTIME1      = '" . trim($line["TIME1"])      . "', 
      @oErrString    = @oErrString output,@O_iErrorState = @O_iErrorState output
  end
 ";
 $line_seq += 16384;
 }
 $intNumRows = mssql_fetch_batch($rset);
}


if(($last_je != "") &&($detail != ""))
{
  $sql = "
  set nocount on
  declare @O_iErrorState int
  declare @oErrString    varchar(255) 

  select @O_iErrorState = 0, @oErrString = '';

  $header
  $detail

  if (@O_iErrorState = 0)
  begin
   exec taCreateUpdateBatchHeaderRcd
    @I_vBACHNUMB='$batch', @I_vSERIES='2', @I_vBCHSOURC='0', @I_vDOCAMT='0', @I_vORIGIN='1', @I_vNUMOFTRX='0', 
    @oErrString = @oErrString output, @O_iErrorState = @O_iErrorState output
  end

  if @O_iErrorState = 0
   begin
     select 'OK' as res;
   end
  else
   begin
     select 'ERR' as res, rtrim(ErrorDesc) as message from DYNAMICS..taErrorCode where errorcode=@O_iErrorState;
   end
  ";

  echo "$sql\n\n\n";
  $xrset = mssql_query($sql, $new_gp);
  $xline = mssql_fetch_assoc($xrset);
  if($xline)
  {
   if($xline["res"] == "OK")
    echo "Imported journal: $last_je, Batch: $batch\n";
   else
    echo "Failed to imported journal: $last_je, Batch: $batch. Reason: " . trim($xline["message"]) . "\n";
    
   mssql_free_result($xrset);
  }
  else
  {
   echo "Failed to imported journal (php): $last_je, Batch: $batch. Reason: " . mssql_get_last_message() . "\n";
  }
}
echo "Completed batch [ $batch ]\n";
}
*/


echo "\n\n\n\n\n";
echo "Connecting to old GP\n";
$old_gp = mssql_connect("192.168.0.25", "expedite", "vagapvek");
mssql_select_db("tcpj", $old_gp);

echo "Connecting to new GP\n";
$new_gp = mssql_connect("192.168.0.119", "sa", "v@g@pv3k");
mssql_select_db("cpj", $new_gp);

echo "\n\n\n\n\n";


##################################################################
#                                                                #
#            Things to setup Manualy                             #
#  1. Enter registration information                             #
#  2. Import GL Codes                                            #
#  3. Setup Currencies and exchange table                        #
#  4. Setup Tax Schedules                                        #
#                                                                #
#                                                                #
#                                                                #
#                                                                #
##################################################################

/*

echo "Importing Company Address Data\n";
table_copy("select 
             CMPANYID, LOCATNID, LOCATNNM, ADRSCODE, ADRCNTCT, ADDRESS1, ADDRESS2, ADDRESS3, CITY, COUNTY, STATE, ZIPCODE, COUNTRY, PHONE1, PHONE2, PHONE3, FAXNUMBR, CHANGEBY_I, CHANGEDATE_I, CCode
             from sy00600 where LOCATNID in ('NCB US$ ACCOUNT','NCB JMD ACCOUNT')",
            "sy00600"
          );
         
echo "Importing Check Book Data\n";
table_copy("select 
             CHEKBKID, DSCRIPTN, BANKID, CURNCYID, BNKACTNM, '00000000000000000001' as NXTCHNUM, '00000000000000000001' as Next_Deposit_Number, INACTIVE, 
             DYDEPCLR, XCDMCHPW, MXCHDLR, DUPCHNUM, OVCHNUM1, LOCATNID, CMUSRDF1, CMUSRDF2, 
             '1900-01-01 00:00:00.000' as Last_Reconciled_Date, '0.00' as Last_Reconciled_Balance, '0.00' as CURRBLNC,  
             Recond, Reconcile_In_Progress, Deposit_In_Progress, CHBKPSWD, CURNCYPD, CRNCYRCD, ADPVADLR, ADPVAPWD, DYCHTCLR, CMPANYID, CHKBKTYP, 
             DDACTNUM, DDINDNAM, DDTRANS, PaymentRateTypeID, DepositRateTypeID, CashInTransAcctIdx
             from CM00100",
            "CM00100"
          );

echo "Importing Credit Card Data\n";
table_copy("select 
             PYBLGRBX, RCVBGRBX, CARDNAME, CBPAYBLE, CBRCVBLE, CKBKNUM1, CKBKNUM2, VENDORID
             from sy03100",
            "sy03100"
          );


echo "Importing Vendor Class Data\n";
table_copy("select 
             VNDCLSID, VNDCLDSC, DEFLTCLS, MXIAFVND, MXINVAMT, WRITEOFF, CREDTLMT, TEN99TYPE, PTCSHACF, MXWOFAMT,
             MINORDER, CRLMTDLR, PYMNTPRI, SHIPMTHD, PYMTRMID, MINPYTYP, MINPYDLR, MINPYPCT,  CURNCYID, TAXSCHID,
             KPCALHST, KGLDSTHS, KPERHIST, KPTRXHST, TRDDISCT, USERDEF1, USERDEF2, 
             CHEKBKID, RATETPID, Revalue_Vendor, Post_Results_To, FREEONBOARD, DISGRPER, DUEGRPER, TaxInvRecvd, CBVAT
             from pm00100",
            "pm00100"
          );

echo "Importing GL code segment setup\n";
table_copy("select SGMTNUMB, SGMTNAME, LOFSGMNT, MXLENSEG, USDFSGKY, MNSEGIND, SegmentWidth from SY00300", "SY00300");

echo "Creating Payment terms\n";
table_copy("
                 select PYMTRMID, DUETYPE, DUEDTDS, DISCTYPE, DISCDTDS, DSCLCTYP, DSCDLRAM, DSCPCTAM, SALPURCH, DISCNTCB, FREIGHT, MISC, TAX, CBUVATMD, USEGRPER
                 from cpj..sy03300
           ", "sy03300");

echo "Creating Shipping methods\n";
table_copy("select SHIPMTHD, SHMTHDSC, PHONNAME, CONTACT, CARRACCT, CARRIER, SHIPTYPE  from cpj..SY03000", "SY03000");


echo "Importing Sales/Purchases Tax Schedule Header Master\n";
table_copy("SELECT TAXSCHID, TXSCHDSC FROM TX00101", "TX00101");

echo "Importing Sales/Purchases Tax Schedule Master\n";
table_copy("SELECT TAXSCHID, TAXDTLID, TXDTLBSE, TDTAXTAX, Auto_Calculate FROM TX00102", "TX00102");

echo "Importing Sales/Purchases Tax Master - Company\n";
table_copy("
                SELECT
                   TAXDTLID, TXDTLDSC, TXDTLTYP, ACTINDX,    TXIDNMBR,      TXDTLBSE, TXDTLPCT,     TXDTLAMT, TDTLRNDG, TXDBODTL, TDTABMIN, TDTABMAX, 
                   TDTAXMIN, TDTAXMAX, TDRNGTYP, TXDTQUAL,   TDTAXTAX,      TXDTLPDC, TXDTLPCH,     TXDXDISC, CMNYTXID, NAME,     CNTCPRSN,
                   ADDRESS1, ADDRESS2, ADDRESS3, CITY,       STATE,         ZIPCODE,  COUNTRY,      PHONE1,   PHONE2,   PHONE3,   FAX,      
                   TXUSRDF1, TXUSRDF2, VATREGTX, TaxInvReqd, TaxPostToAcct, IGNRGRSSAMNT, TDTABPCT 
                 FROM TX00201
           ", 
           "TX00201");

echo "Importing GL Acounts\n";
$sql ="
         select 
               rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) as ACTNUMST,  
               ACTALIAS, ACCTTYPE, ACTDESCR, PSTNGTYP,
               (select ACCATDSC from cpj..GL00102 where GL00102.accatnum = GL00100.accatnum) as CATEGORY,
               ACTIVE, TPCLBLNC, DECPLACS, FXDORVAR, BALFRCLC, USERDEF1, USERDEF2, PostSlsIn, PostIvIn,
               PostPurchIn, PostPRIn, ACCTENTR, USRDEFS1, USRDEFS2
         from cpj..GL00100
      ";
econnect_echo($sql, "ACTNUMST", "taUpdateCreateAccountRcd");

##################################
#                                #
#  At this point we need to      #
#  setup the exchange rates      #
#  manually                      #
#                                #
##################################
echo "Exchange Rate Table\n";
table_copy("select EXGTBLID, CURNCYID, EXCHDATE, TIME1, XCHGRATE, EXPNDATE from dynamics..MC00100 order by dex_row_id", "dynamics..MC00100");



$sql ="
        select 
         rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) as Account_Number, CURNCYID, REVALUE, REVLUHOW, Post_Results_To
        from cpj..MC00200 join gl00100 on gl00100.actindx = MC00200.actindx
        where curncyid <> ''
        order by rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3)
      ";
econnect_copy($sql, "Account_Number", "taCreateAccountCurrencies");






#Customer Class
$sql ="
select
	CLASSID,  CLASDSCR, CRLMTTYP, CRLMTAMT, CRLMTPER, CRLMTPAM, DEFLTCLS, BALNCTYP, CHEKBKID, TAXSCHID, SHIPMTHD, PYMTRMID,
	CUSTDISC, MINPYTYP, MINPYDLR, MINPYPCT, MXWOFTYP, MXWROFAM, FNCHATYP, FINCHDLR, FNCHPCNT, PRCLEVEL, CURNCYID, RATETPID,
	DEFCACTY, SALSTERR, SLPRSNID, STMTCYCL, KPCALHST, KPDSTHST, KPERHIST, KPTRXHST, DISGRPER, DUEGRPER, 
	Revalue_Customer, Post_Results_To, ORDERFULFILLDEFAULT, CUSTPRIORITY, 
        (select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmcshacc) as CASHACCT,
	(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmaracc) as ACCTRECACCT, 
	(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmslsacc) as SALESACCT, 
	(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmcosacc) as COSTOFSALESACCT, 
	(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmivacc) as IVACCT, 
	(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmtakacc) as TERMDISCTAKENACCT, 
	(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmavacc) as TERMDISCAVAILACCT, 
	(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmfcgacc) as FINCHRGACCT,
	(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmwracc) as WRITEOFFACCT, 
	(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmsoracc) as SALESORDERRETACCT, 
	(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = RMOvrpymtWrtoffAcctidx) as RMOvrpymtWrtoffAcct
         from cpj..RM00201  
      ";
econnect_copy($sql, "CLASSID", "taCreateCustomerClass");



echo "\n ---- Creating Sales Territories ----\n";
econnect_copy("select SALSTERR, SLTERDSC, STMGRFNM, STMGRMNM, COUNTRY, KPCALHST, KPERHIST from cpj..rm00303", "SALSTERR", "taCreateTerritory");


echo "\n ---- Creating Sales Reps ----\n";
econnect_copy("
                select
                  SLPRSNID, SALSTERR, EMPLOYID, VENDORID, SLPRSNFN, SPRSNSMN, SPRSNSLN, ADDRESS1, ADDRESS2, ADDRESS3, CITY, STATE, ZIP, COUNTRY, PHONE1, PHONE2, PHONE3, FAX,
                  INACTIVE, COMMCODE, COMPRCNT, STDCPRCT, COMAPPTO, KPCALHST, KPERHIST, MODIFDT, CREATDDT, COMMDEST
                from cpj..rm00301 order by SLPRSNID
              ", 
              "SLPRSNID", 
              "taCreateSalesperson"
             );
/*
echo "\n ---- Creating Vendors ----\n";
econnect_copy("
select 
VENDORID, VENDNAME, VENDSHNM, VNDCHKNM, HOLD,     VENDSTTS, VNDCLSID, VADDCDPR, VNDCNTCT, ADDRESS1, ADDRESS2, ADDRESS3,CITY, STATE, ZIPCODE, CCode, COUNTRY,
PHNUMBR1, PHNUMBR2, FAXNUMBR, TAXSCHID, SHIPMTHD, UPSZONE,  VADCDPAD, VADCDTRO, VADCDSFR, ACNMVNDR, COMMENT1, COMMENT2, CURNCYID, RATETPID,
PYMTRMID, DISGRPER, DUEGRPER, PYMNTPRI, MINORDER, TRDDISCT, TXIDNMBR, TXRGNNUM, CHEKBKID, USERDEF1, USERDEF2, TEN99TYPE, TEN99BOXNUMBER, FREEONBOARD, USERLANG,
MINPYTYP, MINPYDLR, MINPYPCT, MXIAFVND, MAXINDLR, CREDTLMT, CRLMTDLR, WRITEOFF, MXWOFAMT, Revalue_Vendor, Post_Results_To, KPCALHST, KPERHIST, KPTRXHST, KGLDSTHS
PTCSHACF, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmcshidx) as PMCSHACTNUMST,
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmapindx) as PMAPACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmdavidx) as PMDAVACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmdtkidx) as PMDTKACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmfinidx) as PMFINACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmprchix) as PMPRCHACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmtdscix) as PMTDSCACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmmschix) as PMMSCHACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmfrtidx) as PMFRTACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmtaxidx) as PMTAXACTNUMST,
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = pmwrtidx) as PMWRTACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = acpuridx) as ACPURACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = purpvidx) as PURPVACTNUMST
from cpj..PM00200", "VENDORID", "taUpdateCreateVendorRcd");

/*

table_copy("select distinct uscatval, uscatnum, image_url, usercatlongdescr from cpj..iv40600;", "cpj..iv40600");

/*
econnect_copy("
SELECT 
CUSTNMBR, HOLD, INACTIVE, CUSTNAME, SHRTNAME, STMTNAME, CUSTCLAS, CUSTPRIORITY, 
ADRSCODE, CNTCPRSN, ADDRESS1, ADDRESS2, ADDRESS3, CITY, STATE, CCode, COUNTRY, FAX, UPSZONE, SHIPMTHD, TAXSCHID, 
SHIPCOMPLETE, PRSTADCD, PRBTADCD, STADDRCD, SLPRSNID, SALSTERR, USERDEF1, USERDEF2, COMMENT1, COMMENT2, CUSTDISC, PYMTRMID, 
DISGRPER, DUEGRPER, PRCLEVEL, BALNCTYP, FNCHATYP, FNCHPCNT, FINCHDLR, MINPYTYP, MINPYPCT, MINPYDLR, CRLMTTYP, CRLMTAMT, CRLMTPER, CRLMTPAM, 
MXWOFTYP, MXWROFAM, Revalue_Customer, Post_Results_To, ORDERFULFILLDEFAULT, INCLUDEINDP, CRCARDID, CRCRDNUM, CCRDXPDT, BANKNAME, BNKBRNCH, USERLANG, 
TAXEXMT1, TAXEXMT2, TXRGNNUM, CURNCYID, RATETPID, STMTCYCL, KPCALHST, KPERHIST, KPTRXHST, KPDSTHST, 0 as Send_Email_Statements,
CHEKBKID, DEFCACTY, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmcshacc) as RMCSHACTNUMST,  
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmaracc) as RMARACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmslsacc) as RMSLSACTNUMST,  
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmcosacc) as RMCOSACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmivacc) as RMIVACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmtakacc) as RMTAKACTNUMST,  
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmavacc) as RMAVACTNUMST,
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmfcgacc) as RMFCGACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmwracc) as RMWRACTNUMST,
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = rmsoracc) as RMSORACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = RMOvrpymtWrtoffAcctidx) as RMOvrpymtWrtoffACTNUMST, 
GPSFOINTEGRATIONID, INTEGRATIONSOURCE, INTEGRATIONID
from cpj..RM00101 
where INACTIVE = 0 and CUSTNMBR not in ('XXXXXXXXXXXXXXX', '.', 'HSU INTERNAL', 'CPJ INTERNAL', '') and CUSTNAME not like '%INTERSITE%'", "CUSTNMBR", "taUpdateCreateCustomerRcd"); 


//econnect_copy("select UOMSCHDL, UMSCHDSC, BASEUOFM, UMDPQTYS - 1 as UMDPQTYS from IV40201", "UOMSCHDL", "taIVCreateUOFMScheduleHeader");
//econnect_copy("select UOMSCHDL, UOFM, EQUIVUOM, EQUOMQTY, UOFMLONGDESC from IV40202", "UOMSCHDL", "taIVCreateUOFMScheduleLine"); 

//table_copy("select UOMSCHDL, UOFM, SEQNUMBR, EQUIVUOM, EQUOMQTY, UOFMLONGDESC from cpj..IV40202", "cpj..IV40202");

/*

econnect_copy("
select 
ITMCLSCD, ITMCLSDC, DEFLTCLS, ITEMTYPE, ITMTRKOP, LOTTYPE, LOTEXPWARN, LOTEXPWARNDAYS, KPERHIST, KPTRXHST, KPCALHST, KPDSTHST, 
ALWBKORD, ITMGEDSC, TAXOPTNS, ITMTSHID, Purchase_Tax_Options, Purchase_Item_Tax_Schedu, UOMSCHDL, VCTNMTHD, 
USCATVLS_1, USCATVLS_2, USCATVLS_3, USCATVLS_4, USCATVLS_5, USCATVLS_6, DECPLQTY, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivivindx) as Inventory_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivivofix) as Inventory_Offset_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivcogsix) as Cost_of_Goods_Sold_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivslsidx) as Sales_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivsldsix) as Markdowns_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivslrnix) as Sales_Returns_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivinusix) as In_Use_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivinsvix) as In_Service_Acct, 
 --(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivdmgidx) as DamagITEMNMBRed_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivvaridx) as Variance_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = dpshpidx) as Drop_Ship_Items_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = purpvidx) as Purchase_Price_Variance_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = uppvidx)  as Unrealized_Purchase_Price_Variance_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivretidx) as Inventory_Returns_Acct, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = asmvridx) as Assembly_Variance_Acct, 
PRCLEVEL, PriceGroup, PRICMTHD, Revalue_Inventory, Tolerance_Percentage
from IV40400
where ITMCLSCD in (
                        'BS_JUICE','BS_OTHER','BS_SLUSH','BS_SOFTS','BV_ARIZONA','BV_NONALCH','BV_SPIRIT','BV_WINE',
                        'FF_DAIRY','FF_SEAFOOD','GR_BEVERAG','GR_BK GOOD','GR_CANGOOD','GR_COND','GR_NONFOOD','GR_SNACK',
                        'FF_RAW', 'NF_PACKAGI', 'NF_EQUIP', 'WS_POS', 'DELI' 
                   )
", "ITMCLSCD", "taCreateIVItemClass"); 


/*


econnect_copy("
select         
LOCNCODE, LOCNDSCR, ADDRESS1, ADDRESS2, ADDRESS3, CITY, STATE, ZIPCODE, COUNTRY, PHONE1, PHONE2, PHONE3, FAXNUMBR, 
Location_Segment, STAXSCHD, PCTAXSCH, INCLDDINPLNNNG, CCode --, PORECEIPTBIN, PORETRNBIN, SOFULFILLMENTBIN, SORETURNBIN, BOMRCPTBIN, MATERIALISSUEBIN, MORECEIPTBIN, REPAIRISSUESBIN 
from cpj..IV40700", "LOCNCODE", "taCreateInventorySite");           

econnect_copy("select LOCNCODE, BIN from cpj..IV40701", "BIN", "taCreateSiteBin");  


econnect_copy("
select 
ITEMNMBR, ITEMDESC, ITMSHNAM, ITMGEDSC, ITMCLSCD, ITEMTYPE, VCTNMTHD, TAXOPTNS, ITMTSHID, UOMSCHDL, ITEMSHWT, TCC, CNTRYORGN, 
5 as DECPLQTY, 2 as DECPLCUR, Purchase_Tax_Options, Purchase_Item_Tax_Schedu, --STNDCOST, CURRCOST,
ITMTRKOP, LOTTYPE, LOTEXPWARN, LOTEXPWARNDAYS, INCLUDEINDP, MINSHELF1, MINSHELF2, ALWBKORD, WRNTYDYS, ABCCODE, 
USCATVLS_1, USCATVLS_2, USCATVLS_3, USCATVLS_4, USCATVLS_5, USCATVLS_6, KPCALHST, KPERHIST, KPTRXHST, KPDSTHST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivivindx) as IVIVACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivivofix) as IVIVOFACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivcogsix) as IVCOGSACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivslsidx) as IVSLSACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivsldsix) as IVSLDSACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivslrnix) as IVSLRNACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivinusix) as IVINUSACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivinsvix) as IVINSVACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivdmgidx) as IVDMGACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivvaridx) as IVVARACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = dpshpidx) as DPSHPACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = purpvidx) as PURPVACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = uppvidx)  as UPPVACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = ivretidx) as IVRETACTNUMST, 
(select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where actindx = asmvridx) as ASMVRACTNUMST, 
KTACCTSR, PRCHSUOM, Revalue_Inventory, Tolerance_Percentage, LOCNCODE, PRICMTHD, PriceGroup 
from iv00101 
where ITEMDESC != 'Added using Check Links'
order by ITEMNMBR", "ITEMNMBR", "taUpdateCreateItemRcd");
*/

econnect_copy("select itemnmbr, locncode from iv00102 where locncode <> '' order by itemnmbr, locncode;", "itemnmbr", "taItemSite");  

/*
#table_copy("select itemnmbr, locncode, priority, bin, MINSTOCKQTY, MAXSTOCKQTY from iv00117", "iv00117");
#Purchasing UoM
#table_copy("select itemnmbr, BASEUOFM as uofm, 1 as qtybsuom, 3 as umpuropt from iv00101, iv40201 where iv00101.uomschdl = iv40201.uomschdl;", "iv00106");

#table_copy("select PYMTRMID, DUETYPE, DUEDTDS, DISCTYPE, DISCDTDS, DSCLCTYP, DSCDLRAM, DSCPCTAM, SALPURCH, DISCNTCB, FREIGHT, MISC, TAX, CBUVATMD, LSTUSRED, USEGRPER from sy03300 order by DEX_ROW_ID;", "sy03300");



/*
### Recievables Balance ##########
econnect_copy(
 "select CUSTNMBR, DOCNUMBR, DOCDATE, curtrxam as ORTRXAMT, 'rmK_balance' as BACHNUMB, CSHRCTYP, CHEKNMBR, TRXDSCRN, CRCARDID = case when CSHRCTYP=2 then 'VISA- US' else null end, 'USD' as CURNCYID, GLPOSTDT
    from RM20101 where RMDTYPAL = 9 and curtrxam <> 0 and exists (select 1 from rm00103 where custblnc <> 0 and rm00103.custnmbr = rm20101.custnmbr) order by DOCDATE asc", "DOCNUMBR", "taRMCashReceiptInsert");

econnect_copy("select RMDTYPAL, DOCNUMBR, DOCDATE, 'rmL_balance' as BACHNUMB, CUSTNMBR, curtrxam as DOCAMNT, curtrxam as SLSAMNT, trxdscrn as DOCDESCR, 
                      cspornbr as CSTPONBR, PYMTRMID, LSTUSRED, DUEDATE, COMDLRAM, 1 as CREATEDIST, 'USD' as CURNCYID
                from RM20101 where RMDTYPAL <> 9 and curtrxam <> 0 and exists (select 1 from rm00103 where custblnc <> 0 and rm00103.custnmbr = rm20101.custnmbr) order by DOCDATE asc", "DOCNUMBR", "taRMTransaction");


### Payables Balance ##########
econnect_copy("select DOCTYPE, DOCNUMBR, DOCDATE, 'pm_balance' as BACHNUMB, VENDORID, vchrnmbr as VCHNUMWK, curtrxam as DOCAMNT, curtrxam as CHRGAMNT, curtrxam as PRCHAMNT, 'USD' as CURNCYID,
                PSTGDATE, PYMTRMID, DUEDATE, CHEKBKID, TRXDSCRN, PORDNMBR, 1 AS CREATEDIST
                 FROM PM20000 where doctype <> 6 and exists (select 1 from pm00201 where currblnc <> 0 and pm00201.VENDORID = PM20000.VENDORID) order by DOCDATE asc", "DOCNUMBR", "taPMTransactionInsert");


econnect_copy("select VENDORID, DOCNUMBR, DOCDATE, curtrxam as DOCAMNT, 'pm_balance' as BACHNUMB, vchrnmbr as PMNTNMBR, PSTGDATE, PYENTTYP, CARDNAME, 'RBTT US$' as CHEKBKID, TRXDSCRN, 1 as CREATEDIST, 'USD' as CURNCYID
                 FROM PM20000 where doctype = 6 and exists (select 1 from pm00201 where currblnc <> 0 and pm00201.VENDORID = PM20000.VENDORID) order by DOCDATE asc", "DOCNUMBR", "taPMManualCheck");



$inc = "2010";
$sql ="
      select 
            (select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where gl00100.actindx = gl30000.ACTINDX) as ACTNUMST,
            hstyear  as BACHNUMB, JRNENTRY, REFRENCE, TRXDATE, SERIES, CURNCYID, XCHGRATE, RATETPID, TIME1, 
            SOURCDOC, EXCHDATE, CRDTAMNT, DEBITAMT, DSCRIPTN, ORCTRNUM, ORDOCNUM, ORMSTRID, ORMSTRNM, ORTRXTYP, OrigSeqNum, RTCLCMTD, 
            DOCDATE = case when CURNCYID <> 'USD' and docdate = '1900-01-01 00:00:00.000' then TRXDATE else docdate end,
            (select txtfield from SY03900 where SY03900.noteindx = gl30000.noteindx) as NOTETEXT
      from gl30000 where hstyear = '2010' and rctrxseq = 0
      order by JRNENTRY
      ";
echo "Now importing year [ $inc ]\n";
copy_journals($sql, $inc);


$inc = "2011";
$sql ="
      select
            (select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where gl00100.actindx = gl30000.ACTINDX) as ACTNUMST,
            hstyear  as BACHNUMB, JRNENTRY, REFRENCE, TRXDATE, SERIES, CURNCYID, XCHGRATE, RATETPID, TIME1, 
            SOURCDOC, EXCHDATE, CRDTAMNT, DEBITAMT, DSCRIPTN, ORCTRNUM, ORDOCNUM, ORMSTRID, ORMSTRNM, ORTRXTYP, OrigSeqNum, RTCLCMTD, 
            DOCDATE = case when CURNCYID <> 'USD' and docdate = '1900-01-01 00:00:00.000' then TRXDATE else docdate end,
            (select txtfield from SY03900 where SY03900.noteindx = gl30000.noteindx) as NOTETEXT
      from gl30000 where hstyear = '$inc' and rctrxseq = 0
      order by JRNENTRY
      ";
echo "Now importing year [ $inc ]\n";
copy_journals($sql, $inc);

*/

/*
$sql ="
      select
            (select rtrim(actnumbr_1) + '-' + rtrim(actnumbr_2) + '-' + rtrim(actnumbr_3) from gl00100 where gl00100.actindx = gl20000.ACTINDX) as ACTNUMST,
            openyear as BACHNUMB, JRNENTRY, REFRENCE, TRXDATE, SERIES, CURNCYID, XCHGRATE, RATETPID, TIME1, 
            SOURCDOC, EXCHDATE, CRDTAMNT, DEBITAMT, DSCRIPTN, ORCTRNUM, ORDOCNUM, ORMSTRID, ORMSTRNM, ORTRXTYP, OrigSeqNum, RTCLCMTD, 
            DOCDATE = case when CURNCYID <> 'USD' and docdate = '1900-01-01 00:00:00.000' then TRXDATE else docdate end,
            (select txtfield from SY03900 where SY03900.noteindx = gl20000.noteindx) as NOTETEXT
      from gl20000 where rctrxseq = 0
      order by JRNENTRY
      ";
      
echo "Now importing current year\n";
copy_journals($sql, 2012);

*/


?>
