<?php
namespace ElasticsearchORM;

abstract class DataObject extends \ArrayObject
{

    protected $_data = [];

    protected $_cleanData = [];

    protected static $_primary = [];

    protected static $_searchFields = [];

    protected $_id = [];

    protected $_idArray = [];


    public function __construct(array $data = []){
        parent::__construct($data);
        $this->_cleanData = $data;
        if($data && static::$_primary){
            $this->_id = $this->extractIdFromData($data);
        }
    }

    public function offsetSet($fieldName, $value){
        parent::offsetSet($fieldName, $value);
        $this->_data[$fieldName] = $value;
    }

    public function offsetUnset($fieldName){
        parent::offsetUnset($fieldName);
        $this->_data[$fieldName] = null;
    }

    public static function instance(array $data){
        return new static($data);
    }

    public function getCleanData(){
        return $this->_cleanData;
    }

    public static function find(){
        $instance = new static();
        $instance->_id = func_get_args();
        return $instance;
    }

    public static function findArray(array $idArray){
        $instance = new static();
        $instance->_idArray = array_map(function($val){
            return (array) $val;
        }, $idArray);
        return $instance;
    }

    abstract public function fetch($fields=null);
    abstract public function updateFields(array $fields);
    abstract public function delete();
    abstract public function search(array $params);
    abstract public function save();
    abstract public function updateCounter($fieldValues, $defaultValues=null);

    public function increment($field, $size=1){
        $this->updateCounter([$field => $size]);
    }

    public function decrement($field, $size=-1){
        $this->updateCounter([$field => $size]);
    }

    public function load(array $data){
        foreach($data as $k => $v){
            if(isset($this[$k]) && is_array($v)){
                $this[$k] = array_merge($this[$k], $v);
            }else{
                $this[$k] = $v;
            }
        }
    }

    protected function buildScalarId($id){
        if(is_array($id)){
            return implode('_', $id);
        }else{
            return $id;
        }
    }

    public function extractIdFromData($data){
        if(!static::$_primary) throw new \Exception('primary is empty');
        $id = [];
        foreach(static::$_primary as $primary){
            if(!isset($data[$primary])){
                return null;
            }
            $id[] = $data[$primary];
        }
        return $id;
    }


}