[{ /*comment test*/
    _id:"map", 
    value : 
    function(doc) {        
        date = doc.date;
        date.setTime(date.getTime()-date.getTime()%step);
        path = field.split(".");
        for(var i = 0; i<path.length; i++){
            doc = doc[path[i]];
        }
        emit(date, doc);        
    }
},{
    _id:"map_cached", 
    value : 
    function(doc) { 
        var id = {
            date: doc.date, 
            match: hash,
            step: step
        };
        id.date.setTime(id.date.getTime()-id.date.getTime()%step);
        path = field.split(".");
        for(var i = 0; i<path.length; i++){
            doc = doc[path[i]];
        }
        emit(id, doc);
    }
},{
    _id:"count_map", 
    value : 
    function(x) {        
        var date = x.date;
        date.setTime(date.getTime()-date.getTime()%step);
        emit(date, 1);  
    }
},{
    _id:"count_map_cached", 
    value : 
    function(x) { 
        var id = {
            date: x.date, 
            match: hash,
            step: step
        };
        id.date.setTime(id.date.getTime()-id.date.getTime()%step);
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
        var sum = 0;
        for(i = 0; i<values.length; i++){
            sum += values[i];
        }
        return sum; 
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
    _id:"preaggregate_map_inc", 
    value : 
    function(doc) { 
        for(var i = -rangeLeft; i <= rangeRight; i++){
            date = doc.date.getTime() + i*actualMillis;
            id = new Date(date - (date % nextMillis));        
            f = date % nextMillis;
            field =  (f - f % actualMillis) / actualMillis; 
            var output = {};
            output[field] = {sum : doc.value, count : 1, avg : doc.value };
            emit(id, output);        
        }
    }
},{
    _id:"preaggregate_reduce_inc", 
    value :        
    function(id, values){           
        output = {};
        for(i=0; i<nextMillis/actualMillis;i++){
            output[i] = {sum : 0, count : 0, avg : 0};
        }
        for(i = 0; i<values.length; i++){
            doc = values[i];
            for(key in doc){
                output[key].sum += doc[key].sum;
                output[key].count += doc[key].count;
                if(output[key].count != 0)
                    output[key].avg = output[key].sum / output[key].count;
            }
        }
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