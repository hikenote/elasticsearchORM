<?php
namespace ElasticsearchORM;

class ElasticsearchQuery extends Query
{
    protected $_esModel;

    public function __construct($modelClass){
        $this->_esModel = new $modelClass();
    }

    public function assemble(){
        $params = [];
        if($this->_parts['fields']){
            $params['_source'] = $this->_parts['fields'];
        }
        if($this->_parts['skip']){
            $params['from'] = $this->_parts['skip'];
        }
        if($this->_parts['limit']){
            $params['size'] = $this->_parts['limit'];
        }
        if(count($this->_parts['where']) > 1){
            $params['body']['query'] = $this->renderWhere();
        }
        if($this->_parts['multi_match']){
            $params['body']['query']['multi_match'] = $this->_parts['multi_match'];
        }
        if($this->_parts['function_score']){
            $params['body']['query']['function_score'] = $this->_parts['function_score'];
        }
        if($this->_parts['sort']){
            $params['body']['sort'] = $this->renderSort();
        }
        return $params;

    }

    public function renderSort(){
        $sort = [];
        foreach($this->_parts['sort'] as $k => $v){
            $sort[] = [$k => ['order' => $v]];
        }
        if(!isset($this->_parts['_score'])){
            $sort[] = ['_score' => ['order' => 'desc']];
        }
        return $sort;
    }

    public function renderWhere(){
        $where=$this->_parts['where'];
        return [
            'filtered'=> [
                'filter'=>$this->renderFilter($where)
            ],
        ];
    }

    public function renderFilter($childWhere){
        $op=array_shift($childWhere);
        $filter=[];
        if(($op=='and' || $op=='or') && count($childWhere) == 1)
        {
            return $this->renderFilter($childWhere[0]);
        }
        switch($op){
            case '=':
                $filter=['term'=>[$childWhere[0]=>$childWhere[1]]];
                break;
            case '!=':
                array_unshift($childWhere, '=');
                $filter = $this->renderFilter(['not',$childWhere]);
                break;
            case 'not in':
                array_unshift($childWhere, 'in');
                $filter = $this->renderFilter(['not',$childWhere]);
                break;
            case 'exists':
                $filter = ['exists'=>['field'=>$childWhere[0]]];
                break;
            case 'not exists':
                $filter = ['missing'=>['field'=>$childWhere[0]]];
                break;
            case '>':
            case '<':
            case '>=':
            case '<=':
                if($op == '>'){
                    $type = 'gt';
                }else if($op == '<'){
                    $type = 'lt';
                }else if($op == '>='){
                    $type = 'gte';
                }else if($op == '<='){
                    $type = 'lte';
                }
                $filter=['range'=>[$childWhere[0]=>[$type=>$childWhere[1]]]];
                break;
            case 'match':
                $filter=['query'=>['match'=>[$childWhere[0]=>$childWhere[1]]]];
                break;
            case 'in':
                $filter=['terms'=>[$childWhere[0]=>$childWhere[1]]];
                break;
            case 'not':
                foreach($childWhere as $where){
                    $filter['bool']['must_not'][] = $this->renderFilter($where);
                }
                break;
            case 'and':
                foreach($childWhere as $where){
                    $filter['bool']['must'][] = $this->renderFilter($where);
                }
                break;
            case 'or':
                foreach($childWhere as $where){
                    $filter['bool']['should'][] = $this->renderFilter($where);
                }
                break;
            default:
                throw new \Exception('not implement that condition:'.$op);
        }
        return $filter;
    }

    public function search(){
        $result = $this->_esModel->search($this->assemble());
        return $result;
    }

    public function count(){
        $params = $this->_esModel->getParams($this->assemble());
        $params['search_type'] = 'count';
        $result = $this->_esModel->getDbAdapter()->search($params);
        return $result['hits']['total'];
    }

    public function aggregate($aggs){
        $params = $this->_esModel->getParams($this->assemble());
        $params['body']['aggs'] = $aggs;
        $result = $this->_esModel->getDbAdapter()->search($params);
        return $result;
    }


    public function fetchOne(){
        $this->limit(1);
        return $this->search();
    }

}