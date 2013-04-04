[{
    _id:"map", 
    value : 
    function(x) {        
        time = x.time;
        time.setTime(time.getTime()-time.getTime()%step);
        emit(time, x.data[field]);        
    }
},{
    _id:"map_cached", 
    value : 
    function(x) { 
        var id = {
            time: x.time, 
            match: hash,
            step: step
        };
        id.time.setTime(id.time.getTime()-id.time.getTime()%step);
        emit(id, x.data[field]);
    }
},{
    _id:"count_map", 
    value : 
    function(x) {        
        var time = x.time;
        time.setTime(time.getTime()-time.getTime()%step);
        emit(time, 1);  
    }
},{
    _id:"count_map_cached", 
    value : 
    function(x) { 
        var id = {
            time: x.time, 
            match: hash,
            step: step
        };
        id.time.setTime(id.time.getTime()-id.time.getTime()%step);
        emit(id, 1);
    }
},{
    _id:"count_reduce", 
    value : 
    function(id, values) { 
        return values.length; 
    }
},{
    _id:"sum_reduce", 
    value : 
    function( id, values) { 
        var ret = 0;
        for(i = 0; i<values.length; i++){
            ret += values[i];
        }
        return ret; 
    }
},{
    _id:"avg_reduce", 
    value : 
    function(id, values) { 
        var ret = 0;
        for(i = 0; i<values.length; i++){
            ret += values[i];
        }
        return ret/values.length; 
    }
},{
    _id:"min_reduce", 
    value : 
    function(id, values) { 
        return Math.min.apply(Math, values);
    }
},{
    _id:"max_reduce", 
    value : 
    function(id, values) { 
        return Math.max.apply(Math, values);
    }
},{
    _id:"median_reduce", 
    value : 
    function(id, values) { 
        /*space for more effective way of searching median in array (half array sort is sufficient)*/
       values.sort(function(a,b){return a-b});
       return (values.length%2!=0)?values[(1+values.length)/2-1]:(values[values.length/2-1]+values[values.length/2])/2;
    }
},{
    _id:"preaggregate_map", 
    value : 
    function(x) { 
        output = {sum : x.value, count : 1, avg : x.value };
        emit(1, output);        
    }
},{
    _id:"preaggregate_reduce", 
    value :        
    function(id, values){ 
        var sum = 0, count = 0;
        for(i = 0; i<values.length; i++){
            sum += values[i].sum;
            count += values[i].count;
        }
        output = {sum : sum, count : count, avg : sum / count };
        return output; 
    }
},{
    _id:"preaggregate_map_upper", 
    value : 
    function(x) {  
        var sum = 0, count = 0;
        for(field in x.agg){
            sum += x.agg[field].sum;
            count += x.agg[field].count;
        }
        output = {sum : sum, count : count, avg : sum / count };
        emit(1, output);        
    }
}]