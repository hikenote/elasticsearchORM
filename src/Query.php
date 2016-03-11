<?php
namespace ElasticsearchORM;

class Query
{
    protected $_parts = [
        'fields' => [],
        'sort'   => [],
        'skip'   => 0,
        'limit'  => 0,
        'where'  => ['and'],
        'multi_match' => [],
        'function_score' => [],
    ];

    public function select(array $fields){
        $this->_parts['fields'] = $fields;
        return $this;
    }

    public function sort($sort){
        $this->_parts['sort'] = $sort;
        return $this;
    }

    public function skip($skip){
        $this->_parts['skip'] = $skip;
        return $this;
    }

    public function limit($limit){
        $this->_parts['limit'] = $limit;
        return $this;
    }

    public function where($where){
        $this->_parts['where'] = $this->normalizeWhere($where);
        return $this;
    }

    public function andWhere($where, $concat){
        $this->_parts['where'][] = $this->normalizeWhere($where, $concat);
        return $this;
    }

    public function multi_match($keywords, array $fields=[], $search_type = 'best_fields'){
        if(count($fields) > 10){  //不能超过10个字段
            $fields = array_slice($fields, 0, 10);
        }
        $this->_parts['multi_match']['fields'] = $fields;
        $this->_parts['multi_match']['query']  = $keywords;
        $this->_parts['multi_match']['type']   = $search_type;
        return $this;
    }

    public function function_score($query=[], $factor=[], $boost_mode='multiply'){
        if(!$query){
            $query['multi_match'] = $this->_parts['multi_match'];
            $this->_parts['multi_match'] = [];
        }
        $this->_parts['function_score']['query'] = $query;
        if($factor)
            $this->_parts['function_score']['field_value_factor'] = $factor;
        $this->_parts['function_score']['boost_mode'] = $boost_mode;
        return $this;
    }

    private function normalizeAssoc($where)
    {
        $out=[];
        foreach($where as $k=>$v){
            if(is_numeric($k)){
                $out[]=$v;
            }else{
                if(is_array($v)){
                    array_splice($v, 1, 0, $k);
                    $out[] = $v;
                }else{
                    $out[] = ['=',$k,$v];
                }
            }
        }
        return $out;
    }

    public function normalizeWhere($where, $concat='and'){
        $result = [];
        $normalizeAssoc = $this->normalizeAssoc($where);
        foreach($normalizeAssoc as $v){
            if(in_array($v[0],['and','or','not'])){
                $normalized=[array_shift($v)];
                foreach($v as $childWhere){
                    if(array_sum(array_map(function($v){
                            return is_numeric($v)?0:1;
                        },array_keys($childWhere))) ===  0 ){//this is a normalized condition
                        $normalized[]=$childWhere;
                    }else{
                        $normalized[] = $this->normalizeWhere($childWhere);
                    }
                }
                $result[]=$normalized;
            }else{
                $result[]=$v;
            }
        }
        if(count($result)>1){
            array_unshift($result, $concat);
            return $result;
        }else{
            return $result[0];
        }
    }
}