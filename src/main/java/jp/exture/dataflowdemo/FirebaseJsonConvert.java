package jp.exture.dataflowdemo;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class FirebaseJsonConvert {

    private static class FlattenJsonFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = c.element();
            StringBuffer cols = new StringBuffer();

            try {

                JSONParser jsonParser = new JSONParser();
                JSONObject jsonObject = (JSONObject) jsonParser.parse(json);

                JSONObject data_user= (JSONObject) jsonObject.get("user_dim");
                JSONArray data_event= (JSONArray) jsonObject.get("event_dim");

                //user_dimの展開
                String user_id = "";
                String first_open_timestamp_micros = "";

                user_id = (String) data_user.get("user_id");
                try { first_open_timestamp_micros = (String) data_user.get("first_open_timestamp_micros"); } catch(Exception e) {}

                JSONObject device_info = (JSONObject) data_user.get("device_info");
                String device_category = "";
                String mobile_brand_name = "";
                String mobile_model_name = "";
                String mobile_marketing_name = "";
                String device_model = "";
                String platform_version = "";
                String resettable_device = "";
                String device_id = "";
                String user_default_language = "";
                String device_time_zone_offset_seconds = "";
                String limited_ad_tracking = "";        

                try { device_category = (String) device_info.get("device_category");  } catch(Exception e) {}
                try { mobile_brand_name = (String) device_info.get("mobile_brand_name"); } catch(Exception e) {}
                try { mobile_model_name = (String) device_info.get("mobile_model_name"); } catch(Exception e) {}
                try { mobile_marketing_name = (String) device_info.get("mobile_marketing_name"); } catch(Exception e) {}
                try { device_model = (String) device_info.get("device_model");  } catch(Exception e) {}
                try { platform_version = (String) device_info.get("platform_version"); } catch(Exception e) {}
                try { resettable_device = (String) device_info.get("resettable_device"); } catch(Exception e) {}
                try { device_id = (String) device_info.get("device_id"); } catch(Exception e) {}
                try { user_default_language = (String) device_info.get("user_default_language"); } catch(Exception e) {}
                try { device_time_zone_offset_seconds = (String)device_info.get("device_time_zone_offset_seconds"); } catch(Exception e) {}
                try { limited_ad_tracking =  (String) device_info.get("limited_ad_tracking"); } catch(Exception e) {}

                JSONObject geo_info = (JSONObject) data_user.get("geo_info");
                String continent = "";
                String country = "";
                String region = "";
                String city = "";

                try { continent = (String) geo_info.get("continent");  } catch(Exception e) {}
                try { country = (String) geo_info.get("country"); } catch(Exception e) {}
                try { region = (String) geo_info.get("region"); } catch(Exception e) {}
                try { city = (String) geo_info.get("city"); } catch(Exception e) {}

                JSONObject app_info = (JSONObject) data_user.get("app_info");
                String app_version = "";
                String app_instance_id = "";
                String app_store = "";
                String app_platform = "";
                String app_id = "";

                try { app_version = (String) app_info.get("app_version"); } catch(Exception e) {}
                try { app_instance_id = (String) app_info.get("app_instance_id");  } catch(Exception e) {}
                try { app_store = (String) app_info.get("app_store"); } catch(Exception e) {}
                try { app_platform = (String) app_info.get("app_platform"); } catch(Exception e) {}
                try { app_id = (String) app_info.get("app_id"); } catch(Exception e) {}

                JSONObject traffic_source = (JSONObject) data_user.get("traffic_source");
                String user_acquired_campaign = "";
                String user_acquired_medium = "";
                String user_acquired_source = "";

                try { user_acquired_campaign = (String) traffic_source.get("user_acquired_campaign"); } catch(Exception e) {}
                try { user_acquired_medium = (String) traffic_source.get("user_acquired_medium"); } catch(Exception e) {}
                try { user_acquired_source = (String) traffic_source.get("user_acquired_source"); } catch(Exception e) {}

                JSONObject bundle_info = (JSONObject) data_user.get("bundle_info");
                String bundle_sequence_id = "";
                try { bundle_sequence_id = (String) bundle_info.get("bundle_sequence_id"); } catch(Exception e) {}

                JSONObject ltv_info = (JSONObject) data_user.get("ltv_info");
                String revenue = "";
                String currency = "";

                try { revenue = (String) ltv_info.get("revenue"); } catch(Exception e) {}
                try { currency = (String) ltv_info.get("currency"); } catch(Exception e) {}

                //event_dimの展開
                for(int i=0; i<data_event.size(); i++) {
                    JSONObject event_dim = (JSONObject) data_event.get(i);

                    String date = "";
                    String event_name = "";
                    String timestamp_micros = "";

                    try { date = (String) event_dim.get("date");  } catch(Exception e) {}
                    try { event_name = (String) event_dim.get("name"); } catch(Exception e) {}
                    try { timestamp_micros = (String) event_dim.get("timestamp_micros"); } catch(Exception e) {}
                    JSONArray data_event2= (JSONArray) event_dim.get("params");

                    String firebase_screen_class = "";
                    String firebase_screen_id = "";
                    String engagement_time_msec = "";

                    // eventの数（ヒットの数）だけループ
                    for(int j=0; j<data_event2.size(); j++) {  
                        JSONObject params = (JSONObject) data_event2.get(j);

                        String key = "";
                        key = (String) params.get("key");
                        JSONObject value = (JSONObject) params.get("value");

                        if (key.equals("firebase_screen_class")) {
                          try { firebase_screen_class = (String) value.get("string_value"); } catch(Exception e) {}
                        } else if (key.equals("firebase_screen_id")) {
                          try { firebase_screen_id = (String) value.get("int_value"); } catch(Exception e) {}
                        } else if (key.equals("engagement_time_msec")) {
                          try { engagement_time_msec = (String) value.get("int_value"); } catch(Exception e) {}
                        } 

                        // StringBufferに値を詰め込む。最後に改行を入れる。
                        cols.append(user_id);
                        cols.append("\t");
                        cols.append(first_open_timestamp_micros);
                        cols.append("\t");
                        cols.append(device_category);
                        cols.append("\t");
                        cols.append(mobile_brand_name);
                        cols.append("\t");
                        cols.append(mobile_model_name);
                        cols.append("\t");
                        cols.append(mobile_marketing_name);
                        cols.append("\t");
                        cols.append(device_model);
                        cols.append("\t");
                        cols.append(platform_version);
                        cols.append("\t");
                        cols.append(resettable_device);
                        cols.append("\t");
                        cols.append(device_id);
                        cols.append("\t");
                        cols.append(user_default_language);
                        cols.append("\t");
                        cols.append(device_time_zone_offset_seconds);
                        cols.append("\t");
                        cols.append(limited_ad_tracking);
                        cols.append("\t");
                        cols.append(continent);
                        cols.append("\t");
                        cols.append(country);
                        cols.append("\t");
                        cols.append(region);
                        cols.append("\t");
                        cols.append(city);
                        cols.append("\t");
                        cols.append(app_version);
                        cols.append("\t");
                        cols.append(app_instance_id);
                        cols.append("\t");
                        cols.append(app_store);
                        cols.append("\t");
                        cols.append(app_platform);
                        cols.append("\t");
                        cols.append(app_id);
                        cols.append("\t");
                        cols.append(user_acquired_campaign);
                        cols.append("\t");
                        cols.append(user_acquired_medium);
                        cols.append("\t");
                        cols.append(user_acquired_source);
                        cols.append("\t");
                        cols.append(bundle_sequence_id);
                        cols.append("\t");
                        cols.append(revenue);
                        cols.append("\t");
                        cols.append(currency);
                        cols.append("\t");
                        cols.append(date);
                        cols.append("\t");
                        cols.append(event_name);
                        cols.append("\t");
                        cols.append(timestamp_micros);
                        cols.append("\t");
                        cols.append(firebase_screen_class);
                        cols.append("\t");
                        cols.append(firebase_screen_id);
                        cols.append("\t");
                        cols.append(engagement_time_msec);
                        cols.append("\n");
                    }
                } 

            } catch (Exception e) {}    
            String rows = cols.toString();
            c.output(rows);
        }
    }
    
    public interface DflOptions extends PipelineOptions {
    	
        String getInput();
        void setInput(String value);
        
        String getOutput();
        void setOutput(String value);
    }
    
    public static void main(String[] args) {
    		PipelineOptionsFactory.register(DflOptions.class);
    		DflOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DflOptions.class);
        Pipeline p = Pipeline.create(options);
        
        PCollection<String> lines = p.apply("ReadJsonFile", TextIO.read().from(options.getInput()));
        PCollection<String> output = lines.apply("FlattenJsonFile", ParDo.of(new FlattenJsonFn()));
        output.apply("WriteTsvFile", TextIO.write().to(options.getOutput()).withSuffix(".tsv"));

        p.run().waitUntilFinish();
    }
}
