/**
 * Created by Administrator on 2016/10/22 0022.
 */
var sourceBigDataHiveColumnsDataGridRows=new Array();
var sourceBigDataHiveSchema;
var sourceBigDataHiveTable;

function getSourceBigDataHiveAdditionalConf()
{
    if(sourceBigDataHiveSchema==null || sourceBigDataHiveTable==null)
    {
        return null;
    }
    var resultObj=new Object();
    if($("#source-bigdatahivetable-radio").is(':checked'))
    {
        resultObj.type=1;
        resultObj.hiveSchema=sourceBigDataHiveSchema;
        resultObj.hiveTable=sourceBigDataHiveTable;
        var selectedItems = $("input[name='source-bigdatahive-cols-checkbox-selected']:checked");
        var secretItems = $("input[name='source-bigdatahive-cols-checkbox-secret']:checked");
        resultObj.selectedCols=new Array();
        $.each(selectedItems, function (index, item) {
            resultObj.selectedCols[index]=item.value;
        });
        resultObj.secretCols=new Array();
        $.each(secretItems, function (index, item) {
            resultObj.secretCols[index]=item.value;
        });
    }
    else if($("#source-bigdatahivesql-radio").is(':checked'))
    {
        resultObj.type=2;
        resultObj.sql=$("#source-bigdatahive-sql-textarea").textbox('getValue');
    }
    else
    {
        alert("请选择源端为Hive表或Hive SQL.");
        return null;
    }
    return resultObj;
}
function validateSourceBigDataHiveData()
{
    var hiveSchema=$.trim($('#source-bigdatahive-schema-select').combobox('getValue'));
    var hiveTable=$.trim($('#source-bigdatahive-table-textbox').textbox('getValue'));
    if(hiveSchema=='')
    {
        $.messager.show({
            title : '错误',
            msg : '请选择源端Hive的Schema.'
        });

        return false;
    }
    if(hiveTable=='')
    {
        $.messager.show({
            title : '错误',
            msg : '请填写源端Hive的Table..'
        });

        return false;
    }

    return true;
}

function showSourceBigDataHiveColumns()
{
    sourceBigDataHiveSchema=null;
    sourceBigDataHiveTable=null;
    sourceBigDataHiveColumnsDataGridRows.length=0;
    $('#source-bigdatahive-columns-datagrid').datagrid('loadData',sourceBigDataHiveColumnsDataGridRows);

    if(!validateSourceBigDataHiveData())
    {
        return;
    }
    var hiveSchema=$.trim($('#source-bigdatahive-schema-select').combobox('getValue'));
    var hiveTable=$.trim($('#source-bigdatahive-table-textbox').textbox('getValue'));

    $.ajax({type:"post",contentType: "application/json; charset=utf-8",url:"bigdatahivetablecolumnslist.json",data:$.toJSON({"hiveSchema":hiveSchema, "hiveTable":hiveTable}),dataType:"json",success: function(resp){
        if(resp!=null)
        {
            if(resp.error!=null)
            {
                $.messager.show({
                    title : '错误',
                    msg : resp.error
                });
            }
            else
            {
                for(var i=0;i<resp.length;i++)
                {
                    sourceBigDataHiveColumnsDataGridRows[i]={code: resp[i].code,name: resp[i].name};
                }
                $('#source-bigdatahive-columns-datagrid').datagrid('loadData',sourceBigDataHiveColumnsDataGridRows);
                sourceBigDataHiveSchema=hiveSchema;
                sourceBigDataHiveTable=hiveTable;
            }
        }
        else
        {
            alert("错误: 服务器返馈异常. "+resp);
        }

    },error: function (XMLHttpRequest, textStatus, errorThrown) {
        alert("错误: 与服务器通信异常, "+errorThrown+"    textStatus: "+textStatus);
    }});
}
function refreshSourceBigDataHiveComboboxData()
{
    var sourceHiveComboList=new Array();
    $.ajax({type:"post",contentType: "application/json; charset=utf-8", url:"bigdatahiveschemalist.json",dataType:"json",success: function(resp){
        if(resp!=null)
        {
            if(resp.error!=null)
            {
                $.messager.show({
                    title : '错误',
                    msg : resp.error
                });
            }
            else
            {
                for(var i=0;i<resp.length;i++)
                {
                    sourceHiveComboList[i]={code: resp[i].code, name: resp[i].name};
                }
                $('#source-bigdatahive-schema-select').combobox('loadData',sourceHiveComboList);
                $('#source-bigdatahive-schema-select').combobox('setValue','btupayprod');
            }
        }
        else
        {
            alert("错误: 服务器返馈异常. "+resp);
        }

    },error: function (XMLHttpRequest, textStatus, errorThrown) {
        alert("错误: 与服务器通信异常, "+errorThrown+"    textStatus: "+textStatus);
    }});
}
function initSourceBigDataHiveAddConf()
{
    $("#source-bigdatahive-schema-select").combobox({
        width:'180px',
        valueField: 'code',
        textField: 'name'
    });

    $("#source-bigdatahive-table-textbox").textbox({
        width:'180px'
    });

    $('#source-bigdatahive-columns-datagrid').datagrid({
        width:'300px',
        height:'130px',
        columns:[[
            {field:'code',title:'code',hidden:true},
            {field:'name',title:'列名称',width:100},
            {field: 'selected', title: '导出', width: 40,
                formatter: function (value, rec, rowIndex)
                {
                    return "<input type=\"checkbox\" checked name=\"source-bigdatahive-cols-checkbox-selected\" value=\"" + rec.code + "\" >";
                }
            },
            {field: 'secret', title: '脱敏', width: 40,
                formatter: function (value, rec, rowIndex)
                {
                    return "<input type=\"checkbox\" name=\"source-bigdatahive-cols-checkbox-secret\" value=\"" + rec.code + "\" >";
                }
            }
        ]],
        onLoadSuccess: function () {
            $("input[name='source-bigdatahive-cols-checkbox-selected']").unbind().bind("click", function () {
                var items = $("input[name='source-bigdatahive-cols-checkbox-selected']:checked");
                var result = "";
                $.each(items, function (index, item) {
                    result = result + "|" + item.value;
                });
                //alert(result);
            });
            $("input[name='source-bigdatahive-cols-checkbox-secret']").unbind().bind("click", function () {
                var items = $("input[name='source-bigdatahive-cols-checkbox-secret']:checked");
                var result = "";
                $.each(items, function (index, item) {
                    result = result + "|" + item.value;
                });
                //alert(result);
            });
        }
    });

    $('#source-bigdatahive-showcolumns-button').linkbutton({
        //iconCls: 'icon-search',
        text:'显示所有列'
    }).click(function(){
        if ($(this).linkbutton('options').disabled == false) {
            showSourceBigDataHiveColumns();
        }
    });

    refreshSourceBigDataHiveComboboxData();

    $("#source-bigdatahive-sql-textarea").textbox({
        width:'910px',
        height:'100px',
        multiline:true,
        disabled:true
    });

    $("#source-bigdatahivetable-radio").click(function(){
        $("#source-bigdatahive-schema-select").combobox({disabled:false});
        $("#source-bigdatahive-table-textbox").textbox({disabled:false});
        $("#source-bigdatahive-showcolumns-button").linkbutton({disabled:false});
        $("#source-bigdatahive-sql-textarea").textbox({disabled:true});
    });

    $("#source-bigdatahivesql-radio").click(function(){
        $("#source-bigdatahive-sql-textarea").textbox({disabled:false});
        $("#source-bigdatahive-schema-select").combobox({disabled:true});
        $("#source-bigdatahive-table-textbox").textbox({disabled:true});
        $("#source-bigdatahive-showcolumns-button").linkbutton({disabled:true});
    });
}

$( function() {
    initSourceBigDataHiveAddConf();
} );