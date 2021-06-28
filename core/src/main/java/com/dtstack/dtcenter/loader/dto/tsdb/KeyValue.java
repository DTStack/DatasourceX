package com.dtstack.dtcenter.loader.dto.tsdb;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * K、V 对
 *
 * @author ：wangchuan
 * date：Created in 上午10:20 2021/6/24
 * company: www.dtstack.com
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyValue {

    private long timestamp;

    private Object value;

    public double doubleValue(){
        if(value == null){
            throw new DtLoaderException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).doubleValue();
        }
        throw new DtLoaderException("the value is " + value + " can't as double value");
    }

    public float floatValue(){
        if(value == null){
            throw new DtLoaderException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).floatValue();
        }
        throw new DtLoaderException("the value is " + value + " can't as float value");
    }

    public long longValue(){
        if(value == null){
            throw new DtLoaderException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).longValue();
        }
        throw new DtLoaderException("the value is " + value + " can't as long value");
    }

    public int intValue(){
        if(value == null){
            throw new DtLoaderException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).intValue();
        }
        throw new DtLoaderException("the value is " + value + " can't as int value");
    }


    public short shortValue(){
        if(value == null){
            throw new DtLoaderException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).shortValue();
        }
        throw new DtLoaderException("the value is " + value + " can't as short value");
    }

    public byte byteValue(){
        if(value == null){
            throw new DtLoaderException("the value is null");
        }
        if(value instanceof Number){
            return ((Number)value).byteValue();
        }
        throw new DtLoaderException("the value is " + value + " can't as byte value");
    }

    public boolean boolValue(){
        if(value == null){
            throw new DtLoaderException("the value is null");
        }
        return Boolean.parseBoolean(value.toString());
    }

    public char charValue(){
        if(value == null){
            throw new DtLoaderException("the value is null");
        }
        String string = value.toString();
        return string.charAt(0);
    }

    public String stringValue(){
        if(value == null){
            throw new DtLoaderException("the value is null");
        }
        return value.toString();
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
