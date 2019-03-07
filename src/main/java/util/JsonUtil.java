package util;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JsonUtil {

    public static JSONObject mapToJson(Map<String, Set<String>> map){
        JSONObject object = new JSONObject();

        for(Map.Entry<String, Set<String>> entry : map.entrySet()){
            String key = entry.getKey();
            Set<String> set = entry.getValue();
            JSONArray array = new JSONArray();
            for(String s : set){
                array.add(s);
            }
            object.put(key, array);
        }
        return object;
    }

    public static Map<String, Set<String>> jsonToMap(JSONObject object){
        Map<String, Set<String>> map = new HashMap<>();
        Set<String> keySet = object.keySet();
        for(String key : keySet){
            map.put(key, new HashSet<>());
            JSONArray array = (JSONArray) object.get(key);
            for(int i = 0; i < array.size(); i++){
                map.get(key).add((String)array.get(i));
            }
        }
        return map;
    }
}
