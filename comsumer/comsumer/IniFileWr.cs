using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace FilesRdWt.BaseLib
{
    public class IniFileWr
    {
        [DllImport("kernel32")]
        private static extern long WritePrivateProfileString(string section, string key, string val, string filePath);
        [DllImport("kernel32")]
        private static extern int GetPrivateProfileString(string section, string key, string def, StringBuilder retVal, int size, string filePath);
        [DllImport("kernel32.dll")]
        public static extern int Beep(int dwFreq, int dwDuration);

        /// <summary>
        /// 寫ini
        /// </summary>
        /// <param name="inFilename"></param>
        /// <param name="section"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public static void IniFileSetVal(string inFilename, string section, string key, string value)
        {
            WritePrivateProfileString(section, key, value, inFilename);
        }

        /// <summary>
        /// 讀INI
        /// </summary>
        /// <param name="inFilename"></param>
        /// <param name="section"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static string IniFileGetVal(string inFilename, string section, string key)
        {
            var temp = new StringBuilder(255);
            int i = GetPrivateProfileString(section, key, "", temp, 255, inFilename);
            if (i == 0) return "";
            return temp.ToString();
        }
    }
}
