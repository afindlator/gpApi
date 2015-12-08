<?php
/**
 * Created by PhpStorm.
 * User: crusher
 * Date: 07/12/2015
 * Time: 9:29 PM
 */

namespace Impact\GP\Purchasing;

use Impact\Core\GP;

class PODetails
{
    private $gp;

    protected $filters = [];

    public function __construct(GP $gp)
    {
        $this->gp = $gp;
    }

    public function get($filters = [])
    {
        $where = "";

        if (!is_array($filters))
            throw new \Exception("filter must be an array. ".json_encode($filters));

        foreach($filters as $filter => $value)
        {
            if (!empty($where))
                $where .= " and ";

            switch($filter)
            {
                case "since":
                    $where .= "a.dex_row_ts > '$value'";
                    break;
                case "prmdate":
                    $where .= "a.prmdate  = '$value'";
                    break;
                case "ponumber":
                    $where .= "a.ponumber = '$value'";
                    break;
                case "itemnmbr":
                    $where .= "a.itemnmbr = '$value'";
                    break;
                case "vendorid":
                    $where .= "a.vendorid = '$value'";
                    break;
                case "locncode":
                    $where .= "a.locncode = '$value'";
                    break;
                default:
                    throw new \Exception("Invalid filter [ $filter ]");
                    break;
            }
        }

        $where = !empty($where) ? "where $where" : "";

        $sql = "
            select a.PONUMBER, convert(varchar(23), a.PRMDATE, 121) PRMDATE, a.VENDORID, b.VENDNAME, ORD, POLNESTA, ITEMNMBR,
                   ITEMDESC, LOCNCODE, UOFM, UMQTYINB, QTYORDER, QTYCANCE, UNITCOST, EXTDCOST, a.CURNCYID,
                   convert(varchar(23), a.DEX_ROW_TS, 121) DEX_ROW_TS, a.DEX_ROW_ID
            from hsu..pop10110 a
            join hsu..POP10100 b on a.PONUMBER = b.PONUMBER and
                                    a.VENDORID = b.VENDORID
            $where
            order by a.PRMDATE
        ";

        return $this->gp->read($sql);
    }
}
