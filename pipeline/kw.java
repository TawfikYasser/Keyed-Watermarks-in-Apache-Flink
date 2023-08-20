        String datasetInputPath = "";
        String datasetOutputPath = "";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSink<Object> stream = env.addSource(new SourceFunction<Tuple3<Long, Long, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple3<Long, Long, Long>> sourceContext) throws Exception {
                      // i.e. Reading from file
                    }
                    @Override
                    public void cancel() {

                    }
                }).keyBy(v -> v.f0).assignTimestampsAndKeyedWatermarks(KeyedWatermarkStrategy.<Tuple3<Long, Long, Long>,
                        Long>forBoundedOutOfOrderness(Duration.ofMillis(1000)).withTimestampAssigner((event, ts) -> event.f2))
                .window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new ProcessWindowFunction<Tuple3<Long, Long, Long>, Object, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<Tuple3<Long, Long, Long>, Object, Long, TimeWindow>.Context context,
                                        Iterable<Tuple3<Long, Long, Long>> iterable, Collector<Object> collector) throws Exception { // aLong is the window key.
                        int windowSize = ((Collection<?>) iterable).size();
                        System.out.println("[ Window Start = "+context.window().getStart()+" | Current Watermark = "+
                                context.currentKeyedWatermark(aLong) + " | Window Key = "+aLong +" | # of Elements in the Window = "+ windowSize +" | Window End: "+
                                context.window().getEnd() + " ]");
                    }
                }).print();
        env.execute("Keyed Watermarks");
