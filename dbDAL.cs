using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DataLayer {
    public class dbDAL { 
        private static int _timeoutSecond = 60;

        public static DataTable executeProc<T>(params SqlParameter[] prm) {
            SqlConnection cnn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = _timeoutSecond;
            cmd.CommandText = ((SpSource)(typeof(T).GetCustomAttributes(typeof(SpSource), true)[0])).Name;
            cmd.Connection = cnn;
            if (prm != null) {
                foreach (SqlParameter pr in prm) {
                    if (pr.Value == null) {
                        pr.Value = DBNull.Value;
                    }
                    cmd.Parameters.Add(pr);
                }
            }
            DataTable dt = new DataTable();
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            da.Fill(dt);
            return dt;
        }
        public static DataTable executeProc(string spName, params SqlParameter[] prm) {
            SqlConnection cnn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = spName;
            cmd.CommandTimeout = _timeoutSecond;
            cmd.Connection = cnn;
            if (prm != null) {
                foreach (SqlParameter pr in prm) {
                    if (pr.Value == null) {
                        pr.Value = DBNull.Value;
                    }
                    cmd.Parameters.Add(pr);
                }
            }
            DataTable dt = new DataTable();
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            da.Fill(dt);
            return dt;
        }
  

        public static void ExecuteNonQuery<T>(int timeoutSecond = 60, params SqlParameter[] prm) {
            SqlConnection cnn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = ((SpSource)(typeof(T).GetCustomAttributes(typeof(SpSource), true)[0])).Name;
            cmd.Connection = cnn;
            cmd.CommandTimeout = _timeoutSecond;
            if (timeoutSecond > (60 * 5)) {
                cmd.CommandTimeout = timeoutSecond;
            }
            if (prm != null) {
                foreach (SqlParameter pr in prm) {
                    if (pr.Value == null) {
                        pr.Value = DBNull.Value;
                    } else {
                        cmd.Parameters.Add(pr);
                    }
                }
            }
            try {
                cnn.Open();
                cmd.ExecuteNonQuery();
                cnn.Close();
            } catch (Exception ex) {
                cnn.Close();
                throw ex;
            }

        }
        public static void ExecuteNonQuery(string spName, params SqlParameter[] prm) {
            SqlConnection cnn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = spName;
            cmd.CommandTimeout = _timeoutSecond;
            cmd.Connection = cnn;
            if (prm != null) {
                foreach (SqlParameter pr in prm) {
                    cmd.Parameters.Add(pr);
                }
            }
            try {
                cnn.Open();
                cmd.ExecuteNonQuery();
                cnn.Close();
            } catch (Exception ex) {
                cnn.Close();
                throw ex;
            }

        }
 
         
        public static List<T> GetSpListWithCaching<T>(int cacheMinutes=4,string spName,params SqlParameter[] prm) where T : class {
            StringBuilder _cacheKey = new StringBuilder();
            _cacheKey.Append("DAL_" + spName);
            if (prm!=null) { 
                foreach (SqlParameter pr in prm) {
                    _cacheKey.Append(pr.Value);
                }
            }
            List<T> OutList = new List<T>();
            OutList = CacheData.GetCachedList<T>(_cacheKey.ToString());
            if (OutList != null) {
                return OutList;
            } else {
                OutList = new List<T>();
            } 

            SqlConnection cn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = cn;
            cmd.CommandTimeout = _timeoutSecond;
            StringBuilder sb = new StringBuilder();
            cmd.CommandType = CommandType.StoredProcedure;
            if (spName!=null) {
                cmd.CommandText = spName;
            } else {
                cmd.CommandText = ((SpSource)(typeof(T).GetCustomAttributes(typeof(SpSource), true)[0])).Name;
            }  
            if (prm!=null) {
                foreach (SqlParameter pr in prm) {
                    if (pr.Value == null) {
                        pr.Value = DBNull.Value;
                    }
                    cmd.Parameters.Add(pr); 
                }
            } 

            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            da.Fill(dt);  

            T c; 
            foreach (DataRow dr in dt.Rows) {
                c = (T)Activator.CreateInstance(typeof(T));
                PropertyInfo[] pr = c.GetType().GetProperties();
                foreach (DataColumn dc in dt.Columns) {
                    object obj = dr[dc];
                    if (obj == DBNull.Value) continue;
                    string propName = dc.ColumnName;
                    if (GetProperty(c, dc.ColumnName) == null) continue;
                    SetProperty(c, propName, obj);
                }
                OutList.Add(c);
            }
            CacheData.AddCacheData(OutList, _cacheKey.ToString(), cacheMinutes);
            return OutList;
        }

        public static List<T> GetSpList<T>(string spName=null, params SqlParameter[] prm) where T : class { 
            SqlConnection cn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = cn;
            cmd.CommandTimeout = _timeoutSecond; 
            cmd.CommandType = CommandType.StoredProcedure;
            if (spName != null) {
                cmd.CommandText = spName;
            } else {
                cmd.CommandText = ((SpSource)(typeof(T).GetCustomAttributes(typeof(SpSource), true)[0])).Name;
            } 
            if (prm != null) {
                foreach (SqlParameter pr in prm) {
                    if (pr.Value == null) {
                        pr.Value = DBNull.Value;
                    }
                    cmd.Parameters.Add(pr); 
                }
            }

            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            da.Fill(dt);
            List<T> OutList = new List<T>();  
            T c; 
            foreach (DataRow dr in dt.Rows) {
                c = (T)Activator.CreateInstance(typeof(T));
                PropertyInfo[] pr = c.GetType().GetProperties();
                foreach (DataColumn dc in dt.Columns) {
                    object obj = dr[dc];
                    if (obj == DBNull.Value) continue;
                    string propName = dc.ColumnName;
                    if (GetProperty(c, dc.ColumnName) == null) continue;
                    SetProperty(c, propName, obj);
                }
                OutList.Add(c);
            } 
            return OutList;
        }
        public static T GetObject<T>(string sqlQuery, params SqlParameter[] prm) where T : class {
            SqlConnection cn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = cn;
            StringBuilder sb = new StringBuilder();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = _timeoutSecond;
            cmd.CommandText = sqlQuery;
            if (prm!=null) {
                foreach (SqlParameter pr in prm) {
                    if (pr.Value == null) {
                        pr.Value = DBNull.Value;
                    }
                    cmd.Parameters.Add(pr);
                }
            }  
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            da.Fill(dt);
            T c = (T)Activator.CreateInstance(typeof(T));
            foreach (DataRow dr in dt.Rows) {
                PropertyInfo[] pr = c.GetType().GetProperties();
                foreach (DataColumn dc in dt.Columns) {
                    object obj = dr[dc];
                    if (obj == DBNull.Value) continue;
                    string propName = dc.ColumnName;
                    if (GetProperty(c, dc.ColumnName) == null) continue;
                    SetProperty(c, propName, obj);
                }
            }
            return c;
        }
        public static List<T> GetListFromTable<T>(T sourceTable, bool noCriteria = false, int topCriteria = -1) where T : class {
            SqlConnection cn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = cn;
            cmd.CommandTimeout = _timeoutSecond;

            StringBuilder sb = new StringBuilder();
            if (topCriteria > 0) {
                sb.Append("Select top(" + topCriteria.ToString() + ") * from " + sourceTable.GetType().Name + " with(nolock) ");
            } else {
                sb.Append("Select * from " + sourceTable.GetType().Name + " with(nolock) ");
            }
            if (!noCriteria) {
                sb.Append("Where ");
                bool _firstInsert = true;
                PropertyInfo[] props = sourceTable.GetType().GetProperties();
                object propVal;
                foreach (PropertyInfo p in props) {
                    propVal = p.GetValue(sourceTable, null);
                    if (propVal != null && propVal.ToString() != "0") {
                        if (_firstInsert) {
                            sb.Append(p.Name + "='" + propVal.ToString() + "' ");
                            _firstInsert = false;
                        } else {
                            sb.Append("and " + p.Name + "=" + propVal.ToString());
                        }
                    }
                }
            }
            if (sb.ToString().EndsWith("Where ")) {
                string str = sb.ToString().Replace("Where ", "");
                sb.Clear();
                sb.Append(str);
            }
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = sb.ToString();

            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            da.Fill(dt);
            List<T> OutList = new List<T>();
            T c;
            foreach (DataRow dr in dt.Rows) {
                c = (T)Activator.CreateInstance(typeof(T));
                PropertyInfo[] pr = c.GetType().GetProperties();
                foreach (DataColumn dc in dt.Columns) {
                    object obj = dr[dc];
                    if (obj == DBNull.Value) continue;
                    string propName = dc.ColumnName;
                    if (GetProperty(c, dc.ColumnName) == null) continue;
                    SetProperty(c, propName, obj);
                }
                OutList.Add(c);
            }

            return OutList;
        }
        public static T GetSingle<T>(T sourceTable, bool noCriteria = false) where T : class {
            SqlConnection cn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = cn;
            cmd.CommandTimeout = _timeoutSecond;

            StringBuilder sb = new StringBuilder();
            sb.Append("Select top(1) * from " + sourceTable.GetType().Name + " with(nolock) ");
            if (!noCriteria) {
                sb.Append("Where ");
                bool _firstInsert = true;
                PropertyInfo[] props = sourceTable.GetType().GetProperties();
                object propVal;
                foreach (PropertyInfo p in props) {
                    propVal = p.GetValue(sourceTable, null);
                    if (propVal != null && propVal.ToString() != "0") {
                        if (propVal is DateTime) {
                            if (Convert.ToDateTime(propVal) == DateTime.MinValue) {
                                continue;
                            }
                        }
                        if (propVal is bool) {
                            if (Convert.ToBoolean(propVal)) {
                                propVal = "1";
                            } else {
                                propVal = "0";
                            }
                        }
                        if (_firstInsert) {
                            sb.Append(p.Name + "='" + propVal.ToString() + "' ");
                            _firstInsert = false;
                        } else {
                            sb.Append("and " + p.Name + "='" + propVal.ToString() + "'");
                        }
                    }
                }
            }
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = sb.ToString();

            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            da.Fill(dt);
            List<T> OutList = new List<T>();
            T c;
            foreach (DataRow dr in dt.Rows) {
                c = (T)Activator.CreateInstance(typeof(T));
                PropertyInfo[] pr = c.GetType().GetProperties();
                foreach (DataColumn dc in dt.Columns) {
                    object obj = dr[dc];
                    if (obj == DBNull.Value) continue;
                    string propName = dc.ColumnName;
                    if (GetProperty(c, dc.ColumnName) == null) continue;
                    SetProperty(c, propName, obj);
                }
                OutList.Add(c);
                break;
            }
            if (OutList.Count() == 0) {
                return null;
            } else {
                return OutList.FirstOrDefault<T>();
            }

        }
        public static bool Update<T>(T sourceTable) where T : class {
            SqlConnection cn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = cn;
            cmd.CommandTimeout = _timeoutSecond;

            StringBuilder sb = new StringBuilder();
            sb.Append("Update " + sourceTable.GetType().Name + " Set ");
            bool _firstInsert = true;
            PropertyInfo[] props = sourceTable.GetType().GetProperties();
            object propVal;
            string id = string.Empty;
            foreach (PropertyInfo p in props) {
                propVal = p.GetValue(sourceTable, null);
                if (propVal != null && propVal.ToString() != "0") {
                    if (p.Name.ToLower() == "id") {
                        id = propVal.ToString();
                        continue;
                    }
                    if (propVal is DateTime) {
                        if (Convert.ToDateTime(propVal) == DateTime.MinValue) {
                            continue;
                        }
                    }
                    if (propVal is bool) {
                        if (Convert.ToBoolean(propVal)) {
                            propVal = "1";
                        } else {
                            propVal = "0";
                        }
                    }
                    if (_firstInsert) {
                        sb.Append(p.Name + "='" + propVal.ToString() + "' ");
                        _firstInsert = false;
                    } else {
                        sb.Append(", " + p.Name + "='" + propVal.ToString() + "'");
                    }
                }
            }
            sb.Append(" Where ID=" + id);
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
            return true;

        }

        private static void SetProperty(Object R, string propName, object value) {
            var obj = R;
            PropertyInfo fi = obj.GetType().GetProperty(propName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            fi.SetValue(obj, value, null);
        }
        private static object GetProperty(object R, string propName) {
            Type type = R.GetType();
            object result;
            result = type.GetProperty(
                propName,
                BindingFlags.GetProperty |
                BindingFlags.IgnoreCase |
                BindingFlags.Public |
                BindingFlags.Instance
                );
            return result;
        }

        public static string executeScalarValue(string spName, params SqlParameter[] prm) {
            SqlConnection cnn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = spName;
            cmd.CommandTimeout = _timeoutSecond;
            cmd.Connection = cnn;
            if (prm != null) {
                for (int i = 0; i < prm.Length; i++) {
                    if (prm[i].Value != null) {
                        cmd.Parameters.Add(prm[i]);
                    }
                }
            }

            string obj = string.Empty;
            try {
                cnn.Open();
                obj = Convert.ToString(cmd.ExecuteScalar());
                cnn.Close();
            } catch (Exception ex) {
                cnn.Close();
                throw ex;
            }

            return obj;
        }
        public static string executeScalarValue(string rawSql) {
            SqlConnection cnn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = rawSql;
            cmd.Connection = cnn;
            cmd.CommandTimeout = _timeoutSecond;

            string obj = string.Empty;
            try {
                obj = Convert.ToString(cmd.ExecuteScalar());
                cnn.Close();
            } catch (Exception ex) {
                cnn.Close();
                throw ex;
            }

            return obj;
        }
        public static bool executeSql(string sqlString, params SqlParameter[] prm) {
            SqlConnection cnn = new SqlConnection(getCnn);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandTimeout = _timeoutSecond;
            try {
                cmd.CommandType = CommandType.Text;
                cmd.Connection = cnn;
                cmd.CommandText = sqlString;
                if (prm != null) {
                    for (int i = 0; i < prm.Length; i++) {
                        if (prm[i].Value != null) {
                            cmd.Parameters.Add(prm[i]);
                        }
                    }
                }
                cnn.Open();
                cmd.ExecuteNonQuery();
                cnn.Close();
                return true;
            } catch {
                cnn.Close();
                return false;
            }
        }


        public static string getCnn = ConfigurationManager.ConnectionStrings[1].ConnectionString; 
    }
    public class SpSource : Attribute {
        public string Name { get; set; }
        public SpSource(string _name) {
            this.Name = _name;
        }
    }
}
