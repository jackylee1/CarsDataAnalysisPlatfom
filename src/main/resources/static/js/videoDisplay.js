    /*地图样式*/
    var mapStyle = [
        {
            "featureType": "arterial",
            "elementType": "geometry.stroke",
            "stylers": {
                "color": "#444444ff",
                "lightness": 61
            }
        },
        {
            "featureType": "local",
            "elementType": "geometry",
            "stylers": {
                "color": "#ccccccff",
                "lightness": 39
            }
        }
    ];

    /*车辆经纬度*/
    var carPoint = [];

    /*地图 标记 图标 原始经纬度到百度地图经纬度的转换器*/
    var map = null;
    var marker = null;
    var icon = null;
    var convertor = new BMap.Convertor();

    /*视频组件*/
    var video = $('#video');

    /*车辆维度 经度 角度*/
    var latitude = $('#latitude');
    var longitude = $('#longitude');
    var rotation = $('#rotation');

    /*当前视频对应的日志*/
    var logFile = null;

    /*当前视频是否有日志*/
    var hasLog = true;

    /*当前视频对应的日志数组*/
    var logs = [];

    /*当前视频对应日志的长度*/
    var logLen;

    /*当前视频已播放的帧数*/
    var currentFrame = 0;

    /*间隔函数*/
    var interval = null;

    /*zTree目录组件*/
    var zTreeObj = null;

    /*初始化地图*/
    function initMap() {
        carPoint = [116.53952, 39.718876];
        var center = new BMap.Point(116.53952, 39.718876);

        map = new BMap.Map('map');
        map.enableScrollWheelZoom();
        map.centerAndZoom(center, 16);
        map.setMapStyle({
            styleJson:mapStyle
        });

        icon = new BMap.Icon('http://lbsyun.baidu.com/jsdemo/img/car.png', new BMap.Size(52,26),{anchor : new BMap.Size(27, 13)});
        marker = new BMap.Marker(center,{icon: icon});
        marker.setRotation(-90);

        map.addOverlay(marker);

        map.addControl(new BMap.MapTypeControl());
    }

    /*获取日志*/
    function getLog(path) {
        if(null !== interval) {
            clearInterval(interval);
        }
        logFile = "";
        hasLog = false;
        currentFrame = 0;
        logs = [];

        var url = "/hadoop/log_stream?fpath=" + path;
        var ajax = new XMLHttpRequest();
        ajax.open('GET', url, true);
        ajax.onreadystatechange = function() {
            if (ajax.readyState === 4) {
                logFile += ajax.responseText;
            }
        };
        ajax.onload = function() {
            if("" === logFile) {
                clearInterval(interval);
                alert('该视频没有日志!');
                return;
            }
            hasLog = true;
            logs = [];
            $.each(logFile.split("\n"), function(idx, line){
                var splits = line.split(",");
                splits.push(stringToDate(splits[0]).getTime());
                splits[1] = parseFloat(splits[1]);
                splits[2] = parseFloat(splits[2]);
                splits[4] = parseFloat(splits[4]);
                logs.push(splits);
            });
            logLen = logs[0].length;
        }
        ajax.send(null);
    }

    /*视频组件  监听play事件*/
    video.get(0).addEventListener("play", function() {
        setMapHeight();
        if(hasLog === false) {
            return;
        }

        /*定位该视频的帧数*/
        currentFrame = 0;
        var videoTime = Math.round(parseFloat(video.get(0).currentTime) * 1000);
        var startTime = logs[0][logLen - 1];
        var logTime = logs[currentFrame][logLen - 1];
        while(videoTime >= parseInt(logTime - startTime)) {
            currentFrame++;
            logTime = logs[currentFrame][logLen - 1];
        }

        /*设置间隔函数*/
        interval = setInterval(function() {
            var videoTime = Math.round(parseFloat(video.get(0).currentTime) * 1000);
            var startTime = logs[0][logLen - 1];
            var logTime = logs[currentFrame][logLen - 1];
            while(videoTime > parseInt(logTime - startTime)) {
                currentFrame++;
                logTime = logs[currentFrame][logLen - 1];
            }


            rotation.get(0).innerHTML = '角度:' + logs[currentFrame][4];

            marker.setRotation(logs[currentFrame][4] - 90);

            if(logs[currentFrame][2] === carPoint[0] && logs[currentFrame][1] === carPoint[1]) {
                return;
            }

            carPoint = [logs[currentFrame][2], logs[currentFrame][1]];
            var ggPoint = new BMap.Point(logs[currentFrame][2], logs[currentFrame][1]);
            convertor.translate([ggPoint], 1, 5, function (data) {
                if(data.status === 0) {
                    var bdPoint = data.points[0];
                    latitude.html('维度:' + bdPoint.lat);
                    longitude.html('经度:' + bdPoint.lng);

                    marker.setPosition(bdPoint);
                    map.panTo(bdPoint);
                }
            });
        }, 20);
    });

    /*视频组件  监听pause事件*/
    video.get(0).addEventListener("pause" , function() {
        if(hasLog === false) {
            return;
        }
        clearInterval(interval);
    });

    /*初始化ztree目录*/
    function initZtree() {
        var ztree = $('#hdfs-tree');
        var setting ={
            view:{
                selectedMulti: false
            },
            async: {
                enable: true,
                url: "/hadoop/list_dirs",
                autoParam: ["path", "id"],
                dataType : "json",
                type : "get"
            },
            data:{
                simpleData : {
                    enable : true,
                    idKey : "id",
                    pIdKey : "parentId",
                    rootPId:null
                }
            },
            callback: {
                onClick: function(event, treeId, treeNode) {
                    if(treeNode.isParent === false) {
                        loadVideo(treeNode.path);
                    }
                }
            }
        };
        zTreeObj = $.fn.zTree.init(ztree, setting);
    }

    /*浏览器窗口变化的事件绑定*/
    window.onresize = setMapHeight;

    /*页面初次加载完成后的操作*/
    $(document).ready(function(){
        setMapHeight();
        initZtree();
        initMap();
    });

    /*时间组件初始化*/
    $('.form_datetime').datetimepicker({
        language:  'zh-CN',
        weekStart: 1,
        todayBtn:  1,
		autoclose: 1,
		todayHighlight: 1,
		startView: 2,
		forceParse: 0,
        showMeridian: 1
    });

    /*绑定时间搜索事件*/
    $('#search').click(function(){
        $.ajax({
            type: "GET",
            url: "/hadoop/search_by_time",
            data: {
                startTime: $('#start-time').children('input').val(),
                endTime: $('#end-time').children('input').val()
            },
            dataType: "json",
            success: function(data){
                $('#search-ans').empty();
                for(var key in data) {
                    $('#search-ans').append('<li class="list-group-item"><a href="#" onclick=loadVideo("'+ data[key]['path']  + '/")>' + data[key]['name'] + '</a></li>');
                }
            }
        });
    });

    /*载入视频*/
    function loadVideo(path) {
        document.getElementById('video-name').innerHTML = path; /*显示视频路径*/
        $('#video-choose').modal('hide'); /*隐藏模式框*/
        video.attr("src", "/hadoop/video_stream?fpath=" + path); /*加载视频*/
        getLog(path); /*加载日志*/
        return false;
    }

    /*地图与视频组件设置为相同高度*/
    function setMapHeight() {
        $('#map').height(video.height());
    }
    
    function updateHBase() {
        $.ajax({
            type: "GET",
            url: "/hadoop/update_hbase",
            data: {},
            dataType: "json",
            success: function(data){
                if(data['success'] === 'true') {
                    alert('更新HBase数据库成功!');
                }
                else {
                    alert('HBase数据库更新失败!');
                }
            }
        });
    }

    /*字符串转换为日期*/
    function stringToDate(s) {
        s = s.replace(/\s/ig,'');
        s = s.substring(s.length - 23);
	    var d = new Date();
	    d.setFullYear(parseInt(s.substring(0,4),10));
	    d.setMonth(parseInt(s.substring(5,7)-1,10));
	    d.setDate(parseInt(s.substring(8,10),10));
	    d.setHours(parseInt(s.substring(11,13),10));
	    d.setMinutes(parseInt(s.substring(14,16),10));
	    d.setSeconds(parseInt(s.substring(17,19),10));
        d.setMilliseconds(parseInt(s.substring(20,23),10))
	    return d;
    }