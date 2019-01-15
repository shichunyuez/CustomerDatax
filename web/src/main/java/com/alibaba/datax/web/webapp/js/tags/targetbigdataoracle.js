/**
 * Created by Administrator on 2016/10/22 0022.
 */
var targetBigDataOracleColumnsDataGridRows=new Array();
var targetBigDataOracleSchema;
var targetBigDataOracleTable;

function getTargetBigDataOracleAdditionalConf()
{
    if(targetBigDataOracleSchema==null || targetBigDataOracleTable==null)
    {
        return null;
    }
    var resultObj=new Object();
    resultObj.oracleSchema=targetBigDataOracleSchema;
    resultObj.oracleTable=targetBigDataOracleTable;

    var selectedItems = $("input[name='target-bigdataoracle-cols-checkbox-selected']:checked");
    resultObj.selectedCols=new Array();
    $.each(selectedItems, function (index, item) {
        resultObj.selectedCols[index]=item.value;
    });

    return resultObj;
}
function validateTargetBigDataOracleData()
{
    var oracleSchema=$.trim($('#target-bigdataoracle-schema-select').combobox('getValue'));
    var oracleTable=$.trim($('#target-bigdataoracle-table-textbox').textbox('getValue'));
    if(oracleSchema=='')
    {
        $.messager.show({
            title : '错误',
            msg : '请选择目标端Oracle的Schema.'
        });

        return false;
    }
    if(oracleTable=='')
    {
        $.messager.show({
            title : '错误',
            msg : '请填写目标端Oracle的Table..'
        });

        return false;
    }

    return true;
}
function showTargetBgiDataOracleColumns()
{
    targetBigDataOracleSchema=null;
    targetBigDataOracleTable=null;
    targetBigDataOracleColumnsDataGridRows.length=0;
    $('#target-bigdataoracle-columns-datagrid').datagrid('loadData',targetBigDataOracleColumnsDataGridRows);

    if(!validateTargetBigDataOracleData())
    {
        return;
    }
    var oracleSchema=$.trim($('#target-bigdataoracle-schema-select').combobox('getValue'));
    var oracleTable=$.trim($('#target-bigdataoracle-table-textbox').textbox('getValue'));

    $.ajax({type:"post",contentType: "application/json; charset=utf-8",url:"bigdataoracletablecolumnslist.json",data:$.toJSON({"oracleSchema":oracleSchema, "oracleTable":oracleTable}),dataType:"json",success: function(resp){
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
                    targetBigDataOracleColumnsDataGridRows[i]={code: resp[i].code,name: resp[i].name};
                }
                $('#target-bigdataoracle-columns-datagrid').datagrid('loadData',targetBigDataOracleColumnsDataGridRows);
                targetBigDataOracleSchema=oracleSchema;
                targetBigDataOracleTable=oracleTable;
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
function refreshTargetBigDataOracleComboboxData()
{
    var targetOracleComboList=new Array();
    $.ajax({type:"post",contentType: "application/json; charset=utf-8", url:"bigdataoracleschemalist.json",dataType:"json",success: function(resp){
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
                    targetOracleComboList[i]={code: resp[i].code, name: resp[i].name};
                }
                $('#target-bigdataoracle-schema-select').combobox('loadData',targetOracleComboList);
                $('#target-bigdataoracle-schema-select').combobox('setValue','BTUPAYPROD');
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
function initTargetBigDataOracleAddConf()
{
    $("#target-bigdataoracle-schema-select").combobox({
        width:'180px',
        valueField: 'code',
        textField: 'name'
    });

    refreshTargetBigDataOracleComboboxData();

    $("#target-bigdataoracle-table-textbox").textbox({
        width:'180px'
    });

    $('#target-bigdataoracle-showcolumns-button').linkbutton({
        //iconCls: 'icon-search',
        text:'显示所有列'
    }).click(function(){showTargetBgiDataOracleColumns();});

    $('#target-bigdataoracle-columns-datagrid').datagrid({
        width:'200px',
        height:'130px',
        columns:[[
            {field:'code',title:'code',hidden:true},
            {field:'name',title:'列名称',width:100},
            {field: 'selected', title: '导入', width: 40,
                formatter: function (value, rec, rowIndex)
                {
                    return "<input type=\"checkbox\" checked name=\"target-bigdataoracle-cols-checkbox-selected\" value=\"" + rec.code + "\" >";
                }
            }
        ]]
    });
}

$( function() {
    initTargetBigDataOracleAddConf();
} );