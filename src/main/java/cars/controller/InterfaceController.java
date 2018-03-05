package cars.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * 界面操作控制器
 */
@Controller
@RequestMapping(value = "")
public class InterfaceController {
    /**
     * 获取视频展示页面
     * @return 返回视频展示页面
     */
    @RequestMapping("/video")
    public String videoDisplay() {
        return "videoDisplay";
    }

    private Logger logger = LoggerFactory.getLogger(InterfaceController.class);
}
