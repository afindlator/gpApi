<?php
/**
 * Created by PhpStorm.
 * User: crusher
 * Date: 07/12/2015
 * Time: 9:28 PM
 */

namespace Impact\Core;


class GP
{
    protected $host;
    protected $user;
    protected $pass;
    protected $company;

    protected $gp;

    protected $connected = false;

    public function __construct($host, $user, $pass, $company)
    {
        $this->host = $host;
        $this->user = $user;
        $this->pass = $pass;

        $this->company = $company;
    }

    public function open()
    {
        $this->gp = mssql_connect($this->host, $this->user, $this->pass);
        if($this->gp === false)
            throw new \Exception("Error connecting to mssql server. ".mssql_get_last_message());

        mssql_select_db($this->company, $this->gp);

        $this->connected = true;
    }

    public function read($sql)
    {
        $data = [];

        $result = mssql_query($sql, $this->gp , 10000);
        if($result === false)
            throw new \Exception("Error creating sync data");

        $name_count  = mssql_num_fields($result);

        do
        {
            $x = [];
            while ($row = mssql_fetch_assoc($result))
            {
                foreach ($row as $key => $value)
                    $x[strtolower($key)] = trim($value);

                $data[] = $x;
            }
        } while(mssql_fetch_batch($result));

        return $data;
    }

    public function close()
    {
        mssql_close($this->gp);
        $this->connected = false;
    }
}