package com.splout.db.common;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * For convenience, beans will extend this class for having a neat toString() method.
 */
public class BaseBean {

  private final static Log log = LogFactory.getLog(BaseBean.class);

  public String toString() {
    try {
      return BeanUtils.describe(this).toString();
    } catch (IllegalAccessException e) {
      log.error("Error in toString() method", e);
    } catch (InvocationTargetException e) {
      log.error("Error in toString() method", e);
    } catch (NoSuchMethodException e) {
      log.error("Error in toString() method", e);
    }
    return super.toString();
  }
}