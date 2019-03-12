package util;


import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JsonUtil {

    public static JsonObject mapToJson(Map<String, Set<String>> map){
        JsonObject object = new JsonObject();

        for(Map.Entry<String, Set<String>> entry : map.entrySet()){
            String key = entry.getKey();
            Set<String> set = entry.getValue();
            JsonArray array = new JsonArray();
            for(String s : set){
                array.add(s);
            }
            object.add(key, array);
        }
        return object;
    }

    public static JsonObject loadToJson(Map<String, Integer> map){
        JsonObject object = new JsonObject();
        for(Map.Entry<String, Integer> entry : map.entrySet()){
            object.addProperty(entry.getKey(), entry.getValue() + "");
        }
        return object;
    }

    public static Map<String, Set<String>> jsonToMap(JsonObject object){
        Map<String, Set<String>> map = new HashMap<>();
        for(Map.Entry<String, JsonElement> entry : object.entrySet()){
            String key = entry.getKey();
            JsonArray array = entry.getValue().getAsJsonArray();
            map.put(key, new HashSet<>());
            for(int i = 0; i < array.size(); i++){
                map.get(key).add(array.get(i).getAsString());
            }
        }
        return map;
    }

    public static Map<String, Integer> jsonToLoadMap(JsonObject object){
        Map<String, Integer> map = new HashMap<>();
        for(Map.Entry<String, JsonElement> entry : object.entrySet()){
            String key = entry.getKey();
            map.putIfAbsent(key, Integer.valueOf(entry.getValue().getAsString()));
        }
        return map;
    }
}
