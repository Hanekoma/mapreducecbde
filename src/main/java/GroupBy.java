import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class GroupBy extends Configured implements Tool {

    private static String inputTable;
    private static String outputTable;

    private static final String GROUPBY_ATTRIBUTE = "groupbyatt";
    private static final String COLUMN_ATTRIBUTE = "columnatt";
    private static final String FAMILY_ATTRIBUTE = "familyatt";


    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Parameters missing: 'inputTable outputTable [family:]attribute [family:]attribute'");
            System.exit(1);
        }
        inputTable = args[0];
        outputTable = args[1];

        int tablesRight = checkIOTables(args);
        if (tablesRight == 0) {
            int ret = ToolRunner.run(new GroupBy(), args);
            System.exit(ret);
        } else {
            System.exit(tablesRight);
        }
    }


    //============================================================== checkTables
    private static int checkIOTables(String[] args) throws Exception {

        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hba = new HBaseAdmin(config);

        if (!hba.tableExists(inputTable)) {
            System.err.println("Input table does not exist");
            return 2;
        }
        if (hba.tableExists(outputTable)) {
            System.err.println("Output table already exists");
            return 3;
        }
        HTableDescriptor htdInput = hba.getTableDescriptor(inputTable.getBytes());
        HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());
        String searchedFamily = args[2].split(":")[0];
        boolean found = false;
        for (byte[] key : htdInput.getFamiliesKeys()) {
            String currentFamily = new String(key);
            System.out.println("family = " + currentFamily);
            if (!found && currentFamily.equals(searchedFamily)) {
                htdOutput.addFamily(new HColumnDescriptor(key));
                found = true;
            }
        }
        hba.createTable(htdOutput);

        return found ? 0 : 5;
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(HBaseConfiguration.create());
        job.setJarByClass(GroupBy.class);
        job.setJobName("GroupBy");
        String[] familyColumn = args[2].split(":");
        String family = familyColumn[0];
        String column = familyColumn[familyColumn.length == 1 ? 0 : 1];
        job.getConfiguration().setStrings(GROUPBY_ATTRIBUTE, args[3]);
        job.getConfiguration().setStrings(FAMILY_ATTRIBUTE, family);
        job.getConfiguration().setStrings(COLUMN_ATTRIBUTE, column);
        Scan scan = new Scan();
        scan.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable));
        TableMapReduceUtil.initTableMapperJob(inputTable, scan, Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 4;
    }


    //=================================================================== Mapper
    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {

            String groupby = context.getConfiguration().getStrings(GROUPBY_ATTRIBUTE)[0];
            String family = context.getConfiguration().getStrings(FAMILY_ATTRIBUTE)[0];
            String column = context.getConfiguration().getStrings(COLUMN_ATTRIBUTE)[0];

            byte[] columnValueRaw = values.getValue(family.getBytes(), column.getBytes());
            if (columnValueRaw != null) {
                String columnValue = new String(values.getValue(family.getBytes(), column.getBytes()));
                byte[] columnGroupValueRaw = values.getValue(family.getBytes(), groupby.getBytes());
                if (columnGroupValueRaw != null) {
                    String columnGroupValue = new String(values.getValue(family.getBytes(), groupby.getBytes()));
                    context.write(new Text(columnGroupValue), new Text(columnValue));
                }
            }
        }
    }

    //================================================================== Reducer
    public static class Reducer extends TableReducer<Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {

            String groupby = context.getConfiguration().getStrings(GROUPBY_ATTRIBUTE)[0];
            String family = context.getConfiguration().getStrings(FAMILY_ATTRIBUTE)[0];
            String column = context.getConfiguration().getStrings(COLUMN_ATTRIBUTE)[0];


            Integer counter = 0;
            while (inputList.iterator().hasNext()) {
                Text val = inputList.iterator().next();
                counter += Integer.parseInt(val.toString());
            }
            String outputTupleKey = groupby + ":" + key.toString();
            Put put = new Put(outputTupleKey.getBytes());
            put.add(family.getBytes(), column.getBytes(), Integer.toString(counter).getBytes());
            context.write(new Text(outputTupleKey), put);
            context.write(key, put);
        }

    }
}
