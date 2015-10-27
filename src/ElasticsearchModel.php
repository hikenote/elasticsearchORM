<?php
namespace ElasticsearchORM;
use Elasticsearch\Common\Exceptions\BadMethodCallException;
use Elasticsearch\Common\Exceptions\InvalidArgumentException;

class ElasticsearchModel extends DataObject
{
    const ADAPTER = 'adapter';
    const INDEX = 'index';
    const TYPE  = 'type';

    protected static $_esAdapter;

    protected static $_index = null;

    protected static $_type = null;

    public static function setAdapter(\Elasticsearch\Client $adapter){
        static::$_esAdapter = $adapter;
    }

    public static function getAdapter(){
        return static::$_esAdapter;
    }

    public static function setIndex($index){
        static::$_index = $index;
    }

    public static function getIndex(){
        return static::$_index;
    }

    public static function setType($type){
        static::$_type = $type;
    }

    public static function getType(){
        return static::$_type;
    }

    public function getBaseParams(){
        $baseParams = [];
        if(static::$_index){
            $baseParams['index'] = static::$_index;
        }
        if(static::$_type){
            $baseParams['type'] = static::$_type;
        }
        return $baseParams;
    }

    public static function getSearchFields(){
        return static::$_searchFields;
    }

    public static function setDocumentInfo($object, $info){
        $object->_index = $info['_index'];
        $object->_type  = $info['_type'];
    }

    public function getParams(array $params){
        return array_merge($this->getBaseParams(), $params);
    }

    public function get($id, $fields=null){
        $result = self::$_esAdapter->get($this->getParams(['id' => $id, '_source' => $fields]));
        if($result['found']){
            $obj = self::instance($result['_source']);
            self::setDocumentInfo($obj, $result);
            return $obj;
        }
    }

    public function mget($ids, $fields=null){
        $result = self::$_esAdapter->mget($this->getParams(['body' => ['ids' => $ids], '_source' => $fields]));
        $data = [];
        foreach($result['docs'] as $value){
            if($value['found']){
                $obj = self::instance($value['_source']);
                self::setDocumentInfo($obj, $value);
                $data[] = $obj;
            }
        }
        return $data;
    }

    public function search(array $params){
        $params = $this->getParams($params);
        $result = self::$_esAdapter->search($params);
        $data = [];
        $data['total'] = $result['hits']['total'];
        foreach($result['hits']['hits'] as $value){
            $obj = self::instance($value['_source']);
            self::setDocumentInfo($obj, $value);
            $data['data'][] = $obj;
        }
        return $data;
    }

    public static function query(){
        return new ElasticsearchQuery(get_called_class());
    }

    public function id(){
        if(!empty($this->_idArray)){
            $ids=[];
            foreach($this->_idArray as $id){
                $ids[] = $this->buildScalarId($id);
            }
            return implode(',', $ids);
        }
        return $this->buildScalarId($this->_id);
    }

    private function _flatArray($key, $value, $prefix=[]){
        $prefix[] = $key;
        if(!is_array($value)){
            return [implode('.', $prefix) => $value];
        }
        $result = [];
        foreach($value as $k => $val){
            $result = array_merge($result, $this->_flatArray($k, $val, $prefix));
        }
        return $result;
    }

    protected function flatArray(array $mapArray){
        $result = [];
        foreach($mapArray as $key => $value){
            $result = array_merge($result, $this->_flatArray($key, $value));
        }
        return $result;
    }

    protected function setScriptCounter($fieldCounters, $defaultValues=[]){
        $scripts = [];
        $fieldCounters = $this->flatArray($fieldCounters);
        foreach($fieldCounters as $key => $value){
            $scripts[] = sprintf('ctx._source.%s+=%d', $key, $value);
        }
        if($defaultValues){
            return ['script' => implode(';', $scripts), 'params' => $this->flatArray($defaultValues)];
        }
        return ['script' => implode(';', $scripts)];
    }

    public function updateCounter($fieldCounters, $defaultValues=[]){
        return self::$_esAdapter->update($this->getParams(['retry_on_conflict' => 5, 'id' => $this->id(), 'body' => $this->setScriptCounter($fieldCounters, $defaultValues)]));
    }

    public function updateFields(array $fields){
        foreach($fields as $k => $v){
            $this->_data[$k] = $v;
        }
        $this->save();
        return $this;
    }

    public function save(){
        try{
            if(empty($this->_data)){
                $result = self::$_esAdapter->create($this->getParams(['id' => $this->id(), 'body' => $this->_cleanData]));
            }else{
                $result = self::$_esAdapter->update($this->getParams(['id' => $this->id(), 'body' => ['doc' => $this->_data, 'doc_as_upsert' => true]]));
                $this->_data = [];
            }
        }catch (\Exception $e){
            return null;
        }

        return $result;
    }

    public function delete(){
        if(!$this->_id){
            throw new BadMethodCallException('id can not be empty!');
        }
        return self::$_esAdapter->delete($this->getParams(['id' => $this->id()]));
    }

    public static function create($data){
        return new static($data);
    }

    public function setBulkParams($params){
        $params = $this->getParams($params);
        unset($params['timeout']);
        $result = [];
        foreach($params as $k => $v){
            if($k == 'client'){
                continue;
            }
            if($k[0] != '_'){
                $k = '_' . $k;
            }
            $result[$k] = $v;
        }
        return $result;
    }

    protected static function checkBulkResponse($bulkResponse){
        if(!$bulkResponse || $bulkResponse['errors']){
            $errors = [];
            foreach($bulkResponse['items'] as $item){
                foreach($item as $action => $result){
                    if(isset($result['error'])){
                        $errors[] = $result['error'];
                    }
                }
            }
            throw new \InvalidArgumentException('es bulk error:'.print_r($errors,true));
        }
        return $bulkResponse;
    }

    public static function asyncBulk($bulk){
        $result = self::$_esAdapter->bulk(['body' => $bulk, 'client' => ['future' => 'lazy']]);
        $futureFunc = function () use ($result) {
            return self::checkBulkResponse($result);
        };
        return $futureFunc;
    }

    public static function bulk($bulk){
        $result = self::$_esAdapter->bulk(['body' => $bulk]);
        return self::checkBulkResponse($result);
    }

    public static function getPrimaryArray($ids){
        $result = [];
        if(is_array($ids)){
            foreach($ids as $index => $id){
                $result[static::$_primary[$index]] = $id;
            }
        }else{
            $result[static::$_primary[0]] = $ids;
        }

        return $result;
    }

    public static function batchUpdateCounter($bulkData, $async=true){
        $bulk = [];
        foreach($bulkData as $data){
            $obj = new static(static::getPrimaryArray($data['id']));
            $fields = $data['fields'];
            $defaults = isset($data['defaultValues']) ? $data['defaultValues'] : [];
            $bulk[] = ['update' => $obj->setBulkParams(['id' => $obj->id(), 'retry_on_conflict' => 5])];
            $bulk[] = $obj->setScriptCounter($fields, $defaults);
        }
        if($async){
            return static::asyncBulk($bulk);
        }else{
            return static::bulk($bulk);
        }

    }


    public static function batchPartialUpdate($bulkData, $async=true){
        $bulk = [];
        foreach($bulkData as $data){
            $obj = new static(static::getPrimaryArray($data['id']));
            $bulk[] = ['update' => $obj->setBulkParams(['id' => $obj->id(), 'retry_on_conflict' => 5])];
            $bulk[] = ['doc' => $data['fields'], 'doc_as_upsert'=>true];
        }
        if($async){
            return static::asyncBulk($bulk);
        }else{
            return static::bulk($bulk);
        }
    }

    public static function batchInsert($bulkData, $async=true){
        $bulk = [];
        foreach($bulkData as $data){
            $obj = new static(static::getPrimaryArray($data['id']));
            $bulk[] = ['index' => $obj->setBulkParams(['id' => $obj->id()])];
            $bulk[] = $data['fields'];
        }
        if($async){
            return static::asyncBulk($bulk);
        }else{
            return static::bulk($bulk);
        }
    }

    public static function batchDelete($bulkId, $async=true){
        $bulk = [];
        foreach($bulkId as $id){
            $obj = new static(static::getPrimaryArray($id));
            $bulk[] = ['delete' => $obj->setBulkParams(['id' => $obj->id()])];
        }
        if($async){
            return static::asyncBulk($bulk);
        }else{
            return static::bulk($bulk);
        }
    }

    public function fetch($fields=null){
        if($this->_idArray){
            return $this->fetchAll($fields);
        }else{
            return $this->fetchRow($fields);
        }
    }

    public function fetchAll($fields=null){
        $ids = [];
        foreach($this->_idArray as $id){
            $ids[] = $this->buildScalarId($id);
        }
        return $this->mget($ids, $fields);
    }

    public function fetchRow($fields=null){
        return $this->get($this->id(), $fields);
    }

    public function refreshIndex(){
        return self::$_esAdapter->indices()->refresh(['index' => $this->index()]);
    }

}