public class SQLTerm {
    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;
    SQLTerm() {
//        _strTableName = "";
//        _strColumnName = "";
//        _strOperator = "";
//        _objValue = null;
    }

    public boolean compare(Object value){

        if(value.getClass()==String.class)
            value = ((String) value).toLowerCase();
        if(_objValue.getClass()==String.class)
            _objValue = ((String) _objValue).toLowerCase();

        if(_strOperator.equals(">")){
            if(((Comparable)value).compareTo(_objValue) > 0){
                return true;
            }
        } else if (_strOperator.equals(">=")) {
            if(((Comparable)value).compareTo(_objValue) >= 0){
                return true;
            }
        } else if (_strOperator.equals("<")) {
            if(((Comparable)value).compareTo(_objValue) < 0){
                return true;
            }
        } else if (_strOperator.equals("<=")) {
            if(((Comparable)value).compareTo(_objValue) <= 0){
                return true;
            }
        } else if (_strOperator.equals("!=")) {
            if(((Comparable)value).compareTo(_objValue) != 0){
                return true;
            }
        } else if (_strOperator.equals("=")) {
            if(((Comparable)value).compareTo(_objValue) == 0){
                return true;
            }
        }
        return false;
    }
}
